import { runInNewContext, type Context } from 'node:vm';

import { type MessageEvent, WebSocket } from 'ws';
import { nanoid } from 'nanoid';
import {
	type INode,
	type INodeType,
	type ITaskDataConnections,
	WorkflowDataProxy,
	type WorkflowParameters,
} from 'n8n-workflow';
import {
	type IDataObject,
	type IExecuteData,
	type INodeExecutionData,
	type INodeParameters,
	type IRunExecutionData,
	type IWorkflowDataProxyAdditionalKeys,
	Workflow,
	type WorkflowExecuteMode,
} from 'n8n-workflow';

import { RPC_ALLOW_LIST, type AgentMessage, type N8nMessage } from './agent-types';

interface AgentJob<T = unknown> {
	jobId: string;
	settings?: T;
	active: boolean;
	cancelled: boolean;
}

interface JobOffer {
	offerId: string;
	validUntil: bigint;
}

interface DataRequest {
	requestId: string;
	resolve: (data: unknown) => void;
	reject: (error: unknown) => void;
}

interface RPCCall {
	callId: string;
	resolve: (data: unknown) => void;
	reject: (error: unknown) => void;
}

const VALID_TIME_MS = 1000;
const VALID_EXTRA_MS = 100;

class Agent {
	id: string;

	ws: WebSocket;

	canSendOffers = false;

	runningJobs: Record<AgentJob['jobId'], AgentJob> = {};

	offerInterval: NodeJS.Timeout | undefined;

	openOffers: Record<JobOffer['offerId'], JobOffer> = {};

	dataRequests: Record<DataRequest['requestId'], DataRequest> = {};

	rpcCalls: Record<RPCCall['callId'], RPCCall> = {};

	constructor(
		public jobType: string,
		private wsUrl: string,
		private maxConcurrency: number,
		public name?: string,
	) {
		this.id = nanoid();
		this.ws = new WebSocket(this.wsUrl + '?id=' + this.id);
		this.ws.addEventListener('message', this._wsMessage);
		this.ws.addEventListener('close', this.stopJobOffers);
	}

	private _wsMessage = (message: MessageEvent) => {
		const data = JSON.parse(message.data as string) as N8nMessage.ToAgent.All;
		void this.onMessage(data);
	};

	private stopJobOffers = () => {
		this.canSendOffers = false;
		if (this.offerInterval) {
			clearInterval(this.offerInterval);
			this.offerInterval = undefined;
		}
	};

	private startJobOffers() {
		this.canSendOffers = true;
		if (this.offerInterval) {
			clearInterval(this.offerInterval);
		}
		this.offerInterval = setInterval(this.sendOffers.bind(this), 250);
	}

	deleteStaleOffers() {
		for (const key of Object.keys(this.openOffers)) {
			if (this.openOffers[key].validUntil < process.hrtime.bigint()) {
				delete this.openOffers[key];
			}
		}
	}

	sendOffers() {
		this.deleteStaleOffers();

		const offersToSend =
			this.maxConcurrency -
			(Object.values(this.openOffers).length + Object.values(this.runningJobs).length);

		if (offersToSend > 0) {
			for (let i = 0; i < offersToSend; i++) {
				const offer: JobOffer = {
					offerId: nanoid(),
					validUntil:
						process.hrtime.bigint() + BigInt((VALID_TIME_MS + VALID_EXTRA_MS) * 1_000_000), // Adding a little extra time to account for latency
				};
				this.openOffers[offer.offerId] = offer;
				this.send({
					type: 'agent:joboffer',
					jobType: this.jobType,
					offerId: offer.offerId,
					validFor: VALID_TIME_MS,
				});
			}
		}
	}

	send(message: AgentMessage.ToN8n.All) {
		this.ws.send(JSON.stringify(message));
	}

	onMessage(message: N8nMessage.ToAgent.All) {
		console.log({ message });
		switch (message.type) {
			case 'n8n:inforequest':
				this.send({
					type: 'agent:info',
					name: this.name ?? 'Node.js Agent SDK',
					types: [this.jobType],
				});
				break;
			case 'n8n:agentregistered':
				this.startJobOffers();
				break;
			case 'n8n:jobofferaccept':
				this.offerAccepted(message.offerId, message.jobId);
				break;
			case 'n8n:jobcancel':
				this.jobCancelled(message.jobId);
				break;
			case 'n8n:jobsettings':
				void this.receivedSettings(message.jobId, message.settings);
				break;
			case 'n8n:jobdataresponse':
				this.processDataResponse(message.requestId, message.data);
				break;
			case 'n8n:rpcresponse':
				this.handleRpcResponse(message.callId, message.status, message.data);
		}
	}

	processDataResponse(requestId: string, data: unknown) {
		const request = this.dataRequests[requestId];
		if (!request) {
			return;
		}
		delete this.dataRequests[requestId];
		request.resolve(data);
	}

	hasOpenJobs() {
		return Object.values(this.runningJobs).length < this.maxConcurrency;
	}

	offerAccepted(offerId: string, jobId: string) {
		if (!this.hasOpenJobs()) {
			this.send({
				type: 'agent:jobrejected',
				jobId,
				reason: 'No open job slots',
			});
			return;
		}
		const offer = this.openOffers[offerId];
		if (!offer) {
			if (!this.hasOpenJobs()) {
				this.send({
					type: 'agent:jobrejected',
					jobId,
					reason: 'Offer expired and no open job slots',
				});
				return;
			}
		} else {
			delete this.openOffers[offerId];
		}

		this.runningJobs[jobId] = {
			jobId,
			active: false,
			cancelled: false,
		};

		this.send({
			type: 'agent:jobaccepted',
			jobId,
		});
	}

	jobCancelled(jobId: string) {
		const job = this.runningJobs[jobId];
		if (!job) {
			return;
		}
		job.cancelled = true;
		if (job.active) {
			// TODO
		} else {
			delete this.runningJobs[jobId];
		}
		this.sendOffers();
	}

	jobErrored(jobId: string, error: unknown) {
		this.send({
			type: 'agent:joberror',
			jobId,
			error,
		});
		delete this.runningJobs[jobId];
	}

	jobDone(jobId: string, data: AgentMessage.ToN8n.JobDone['data']) {
		this.send({
			type: 'agent:jobdone',
			jobId,
			data,
		});
		delete this.runningJobs[jobId];
	}

	async receivedSettings(jobId: string, settings: unknown) {
		const job = this.runningJobs[jobId];
		if (!job) {
			return;
		}
		if (job.cancelled) {
			delete this.runningJobs[jobId];
			return;
		}
		job.settings = settings;
		job.active = true;
		try {
			const data = await this.executeJob(job);
			this.jobDone(jobId, data);
		} catch (e) {
			if ('message' in (e as Error)) {
				this.jobErrored(jobId, (e as Error).message);
			} else {
				this.jobErrored(jobId, e);
			}
		}
	}

	// eslint-disable-next-line @typescript-eslint/no-unused-vars
	async executeJob(job: AgentJob): Promise<AgentMessage.ToN8n.JobDone['data']> {
		throw new Error('Unimplemented');
	}

	async requestData<T = unknown>(
		jobId: AgentJob['jobId'],
		type: AgentMessage.ToN8n.JobDataRequest['requestType'],
		param?: string,
	): Promise<T> {
		const requestId = nanoid();

		const p = new Promise((resolve, reject) => {
			this.dataRequests[requestId] = {
				requestId,
				resolve,
				reject,
			};
		});

		this.send({
			type: 'agent:jobdatarequest',
			jobId,
			requestId,
			requestType: type,
			param,
		});

		return p as T;
	}

	async makeRpcCall(jobId: string, name: AgentMessage.ToN8n.RPC['name'], params: unknown[]) {
		const callId = nanoid();

		const dataPromise = new Promise((resolve, reject) => {
			this.rpcCalls[callId] = {
				callId,
				resolve,
				reject,
			};
		});

		this.send({
			type: 'agent:rpc',
			callId,
			jobId,
			name,
			params,
		});

		try {
			return await dataPromise;
		} finally {
			delete this.rpcCalls[callId];
		}
	}

	handleRpcResponse(
		callId: string,
		status: N8nMessage.ToAgent.RPCResponse['status'],
		data: unknown,
	) {
		const call = this.rpcCalls[callId];
		if (!call) {
			return;
		}
		if (status === 'success') {
			call.resolve(data);
		} else {
			call.reject(typeof data === 'string' ? new Error(data) : data);
		}
	}

	buildRpcCallObject(jobId: string) {
		const rpcObject: any = {};
		for (const r of RPC_ALLOW_LIST) {
			const splitPath = r.split('.');
			let obj = rpcObject;

			splitPath.forEach((s, index) => {
				if (index !== splitPath.length - 1) {
					obj[s] = {};
					obj = obj[s];
					return;
				}
				// eslint-disable-next-line
				obj[s] = (...args: unknown[]) => this.makeRpcCall(jobId, r, args);
			});
		}
		return rpcObject;
	}
}

interface JSExecSettings {
	code: string;

	// For workflow data proxy
	mode: WorkflowExecuteMode;
}

export interface AllData {
	workflow: Omit<WorkflowParameters, 'nodeTypes'>;
	inputData: ITaskDataConnections;
	node: INode;

	runExecutionData: IRunExecutionData;
	runIndex: number;
	itemIndex: number;
	activeNodeName: string;
	connectionInputData: INodeExecutionData[];
	siblingParameters: INodeParameters;
	mode: WorkflowExecuteMode;
	executeData?: IExecuteData;
	defaultReturnRunIndex: number;
	selfData: IDataObject;
	contextNodeName: string;
}

const getAdditionalKeys = (): IWorkflowDataProxyAdditionalKeys => {
	return {};
};

class JsAgent extends Agent {
	constructor(jobType: string, wsUrl: string, maxConcurrency: number, name?: string) {
		super(jobType, wsUrl, maxConcurrency, name ?? 'Test Agent');
	}

	async executeJob(job: AgentJob<JSExecSettings>): Promise<AgentMessage.ToN8n.JobDone['data']> {
		const allData = await this.requestData<AllData>(job.jobId, 'all');

		const settings = job.settings!;

		const workflowParams = allData.workflow;
		const workflow = new Workflow({
			...workflowParams,
			nodeTypes: {
				getByNameAndVersion() {
					return undefined as unknown as INodeType;
				},
				getByName() {
					return undefined as unknown as INodeType;
				},
				getKnownTypes() {
					return {};
				},
			},
		});

		const dataProxy = new WorkflowDataProxy(
			workflow,
			allData.runExecutionData,
			allData.runIndex,
			allData.itemIndex,
			allData.activeNodeName,
			allData.connectionInputData,
			allData.siblingParameters,
			settings.mode,
			getAdditionalKeys(),
			allData.executeData,
			allData.defaultReturnRunIndex,
			allData.selfData,
			allData.contextNodeName,
		);

		const context: Context = {
			require,
			module: {},

			...dataProxy.getDataProxy(),
			...this.buildRpcCallObject(job.jobId),
		};

		const result = (await runInNewContext(
			`module.exports = async function() {${job.settings!.code}\n}()`,
			context,
		)) as AgentMessage.ToN8n.JobDone['data'];

		return result;
	}
}

new JsAgent('javascript', 'ws://localhost:5678/rest/agents/_ws', 5);
