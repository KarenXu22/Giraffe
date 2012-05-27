#include "logic/GPaxosComm.h"
#include "GPaxosProtocol.h"

GPaxosProtocol::GPaxosProtocol()
{
	m_bInitialized = false;
	m_paxosComm = new GPaxosComm(this);
	m_curTime = PUtils::localMilliSeconds();
	PSettings & settings = PSettings::getInstance();
	m_paxosSyncLimit = settings.getConfig("giraffe-paxos-sync-interval").m_value.intvalue;
	S64 leaderLeasePeriod = settings.getConfig("giraffe-leader-lease-interval").m_value.intvalue;
//	assert(leaderLeasePeriod <= m_paxosSyncLimit);

	string GRF_mode = settings.getConfig("giraffe-mode").m_value.strvalue;
	if(GRF_mode != "CLUSTER")
		m_is_cluster = false;
	else
		m_is_cluster = true;

	m_pPaxosState = new GPaxosState(m_paxosComm);

	m_pElectionInstance = new GPaxosLeaderElection(m_pPaxosState);
	m_pLeaderInstance = new GPaxosLeader(m_pPaxosState, m_pElectionInstance);
	m_pFollowerInstance = new GPaxosFollower(m_pPaxosState, m_pElectionInstance);
	m_bInitialized = true;
}

GPaxosProtocol::~GPaxosProtocol()
{

}

void GPaxosProtocol::checkState()
{
	S64 time = PUtils::localMilliSeconds();
	if(time - m_curTime >=  m_paxosSyncLimit)
	{
		m_curTime = time;
		if (m_bInitialized&&m_is_cluster)
		{
			switch(m_pPaxosState->m_state)
			{
			case PAXOS_STATE::LOOKING:
				m_pElectionInstance->electing();
				break;

			case PAXOS_STATE::LEADING:
				{
					m_pLeaderInstance->renewFollowerMembership();
					//产生Prepare事件供给测试
				    //static U32 test_counter = 0;
					//if(test_counter<=1)
					//{
					//	U64	test_prepareTxid =TXID::encodeID(m_pPaxosState->m_iCurrentEpoch,test_counter);
						string test_prepareString = "手动发送的测试Prepare事件！";
					//	cout<<"m_pPaxosState->m_iCurrentEpoch="<<m_pPaxosState->m_iCurrentEpoch<<"    m_pPaxosState->m_iTxCounter="<<m_pPaxosState->m_iTxCounter<<"     m_pPaxosState->m_iTxCounter="<<m_pPaxosState->m_iCurrentTxid<<endl;
					//	cout<<"m_pPaxosState->m_iCurrentEpoch="<<m_pPaxosState->m_iCurrentEpoch<<"                   test_counter="<<test_counter<<"                test_prepareTxid="<<test_prepareTxid<<endl;
						cout<<endl<<endl<<"*************************************************"<<endl;
						cout<<"新一轮多播开始了！！！！！！！！！！"<<endl;
						cout<<"产生一个Prepare事件:"<<"txid="<<m_pPaxosState->m_iCurrentTxid<<"   epoch="<<m_pPaxosState->m_iCurrentEpoch<<"  counter="<<m_pPaxosState->m_iTxCounter<<endl;
						PaxosEvent test_preparePkt= PaxosEvent(PAXOS_EVENT::UAB_PREPARE_EVENT, m_pPaxosState->m_iMyid, m_pPaxosState->m_iCurrentEpoch, m_pPaxosState->m_iCurrentTxid, NULL, test_prepareString);
						m_pLeaderInstance->handleEvent(test_preparePkt);
						cout<<endl<<endl;
				  // }
				}
				break;

			case PAXOS_STATE::FOLLOWING:
				m_pFollowerInstance->pingToLeader();
				break;

			default:
				assert(true);
			}
		}
	}	
}

void GPaxosProtocol::dispatchPaxosEvent(PaxosEvent e)
{
	//m_paxos->paxos_event_disPatch(e);
	unsigned eventType = e.m_iEventType;

	switch(eventType)
	{
	case PAXOS_EVENT::PREPARE_EVENT:
	case PAXOS_EVENT::PROMISE_EVENT:
	case PAXOS_EVENT::ACCEPT_EVENT:
	case PAXOS_EVENT::ACCEPTED_EVENT:
	case PAXOS_EVENT::LEADER_EVENT:
	case PAXOS_EVENT::IS_LEADER_EVENT:
		m_pElectionInstance->handlePaxosEvent(e);
		break;

	case PAXOS_EVENT::PING_FOLLOWER_EVENT:
	case PAXOS_EVENT::UAB_PROPOSAL_EVENT:
	case PAXOS_EVENT::UAB_COMMIT_EVENT:
		m_pFollowerInstance->handleEvent(e);
		break;

	case PAXOS_EVENT::PING_LEADER_EVENT:
	case PAXOS_EVENT::UAB_PREPARE_EVENT:
	case PAXOS_EVENT::UAB_ACK_EVENT:
		m_pLeaderInstance->handleEvent(e);
		break;

	default:
		break;
	}
}

void GPaxosProtocol::setAppTerminate()
{
	m_paxosComm->setAppTerminate();
}
