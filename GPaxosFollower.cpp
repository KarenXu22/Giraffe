#include "GPaxosUtils.h"
#include "GPaxosState.h"
#include "GPaxosLeaderElection.h"
#include "logic/GPaxosComm.h"
#include "GPaxosFollower.h"
#include "Packets/Giraffe/paxos/PGRFPaxosPackets.h"

GPaxosFollower::GPaxosFollower(GPaxosState* paxos_state, GPaxosLeaderElection* election )
{
	m_bStop = true;
	m_pPaxosState = paxos_state;
	m_pComm = m_pPaxosState->getComm();
	assert(election);
	m_pElectionInstance = election;
	
	PSettings & settings = PSettings::getInstance();
	m_sLeaderLeasePeriod = settings.getConfig("giraffe-leader-lease-interval").m_value.intvalue;
}

GPaxosFollower::~GPaxosFollower()
{

}

void GPaxosFollower::handleEvent(PaxosEvent pe)//follower接收到的事件
{
	if (m_pPaxosState->m_state != PAXOS_STATE::FOLLOWING)
	{
		//print warning
		cout << "unexpected event event type:" << GPaxosUtils::strEventType(pe.m_iEventType) << endl;
		return;
	}

	unsigned eventType = pe.m_iEventType;

	switch(eventType)
	{
	case PAXOS_EVENT::PING_FOLLOWER_EVENT:
		cout <<  " get leader ping"<< endl;
		m_sLastPingFromLeader = PUtils::localMilliSeconds();
		break;
	case PAXOS_EVENT::UAB_PROPOSAL_EVENT:
		handleProposalEvent(pe);
		break;

	case PAXOS_EVENT::UAB_COMMIT_EVENT:
		handleCommitEvent(pe);
		break;

	default:
		assert(0);
		break;
	}
}

void GPaxosFollower::pingToLeader()
{
	if (!m_bStop)
	{
		S64 time = PUtils::localMilliSeconds();
		if (time - m_sLastPingFromLeader <= m_sLeaderLeasePeriod)
		{
			cout << "time:" << time - m_sLastPingFromLeader << "period" << m_sLeaderLeasePeriod << endl; 
			cout << "ping to leader, epoch:" << m_pPaxosState->m_iLeaderEpoch<< "    leader id:" <<m_pPaxosState->m_iLeaderId << endl;
			PPacketBase* pingPkt = (PGRFPaxosLeaderKeepAliveReq*)new PGRFPaxosLeaderKeepAliveReq();
			//PPacketBase* pingPkt = (PGRFPaxosEventPkt*)new PGRFPaxosEventPkt(PAXOS_EVENT::PING_LEADER_EVENT, m_pPaxosState->m_iMyid, m_pPaxosState->m_iCurrentEpoch, m_pPaxosState->m_iCurrentTxid, m_pPaxosState->m_oLocalPaxosAddr);
			m_pComm->tcpSend(m_pPaxosState->m_oLeaderAddr, pingPkt);

		}
		else//leader time out
		{
			startLeaderElection();
			m_bStop = true;
		}
	}
	else
	{
			m_sLastPingFromLeader = PUtils::localMilliSeconds();
			PPacketBase* pingPkt = (PGRFPaxosLeaderKeepAliveReq*)new PGRFPaxosLeaderKeepAliveReq();
			//PPacketBase* pingPkt = (PGRFPaxosEventPkt*)new PGRFPaxosEventPkt(PAXOS_EVENT::PING_LEADER_EVENT, m_pPaxosState->m_iMyid, m_pPaxosState->m_iCurrentEpoch, m_pPaxosState->m_iCurrentTxid, m_pPaxosState->m_oLocalPaxosAddr);
			m_pComm->tcpSend(m_pPaxosState->m_oLeaderAddr, pingPkt);
			m_bStop = false;
	}
}

void GPaxosFollower::startLeaderElection()
{
	cout << "leader time out!!! look for new leader!!" <<endl;
	m_pPaxosState->setLookerState();//reset state, increase epoch number 
	m_pElectionInstance->Initialize();// reset proposal issue
}

void GPaxosFollower::handleProposalEvent(PaxosEvent& pe)
{
	cout<<endl<<endl;
	TXID paxosDecodeTxid =TXID::decodeID(pe.m_lTxid);
	if(pe.m_iEpoch!=paxosDecodeTxid.m_epoch || pe.m_iEpoch!= m_pPaxosState->m_iCurrentEpoch)
	{
		cout << "get invalid proposal event from leader" << endl;
		//may handle exception event here
		return;
	}
    cout << "接收到一个有效的 Proposal 事件: txid=" << pe.m_lTxid<< endl;
	/*if(pe.m_lTxid > m_pPaxosState->m_iCurrentTxid)
	{
		cout<<"设置m_iCurrentTxid为:"<<pe.m_lTxid<<endl;
		m_pPaxosState->m_iCurrentTxid = pe.m_lTxid;
	}*/

	//send ack to leader, and should write event to log and add event to commit_waiting queue
	//日志部分
	//cout<<"m_pPaxosState->m_iCurrentTxid"<<m_pPaxosState->m_iCurrentTxid<<"    m_pPaxosState->m_iTxCounter="<<m_pPaxosState->m_iTxCounter<<endl;
	PPacketBase* pingPkt = (PGRFBroadcastEventPkt*)new PGRFBroadcastEventPkt(PAXOS_EVENT::UAB_ACK_EVENT, m_pPaxosState->m_iMyid, pe.m_iEpoch, pe.m_lTxid, "ack packet");
	cout<<"发送ACK给leader"<<endl;
	m_pComm->tcpSend(pe.m_oFrmAddr, pingPkt);
	cout<<endl<<endl;
}

void GPaxosFollower::handleCommitEvent(PaxosEvent& pe)
{
	cout<<endl<<endl;
	//m_pPaxosState->m_iCurrentTxid = pe.m_lTxid;
	//validate the commit event 
	//if not ok, handle exception
	//else
	//MaxCommitTxid +1; set log item true and write value into fs
	TXID paxosDecodeTxid =TXID::decodeID(pe.m_lTxid);
	if(pe.m_iEpoch != m_pPaxosState->m_iCurrentEpoch || paxosDecodeTxid.m_epoch != pe.m_iEpoch)
	{
		cout<<"the commit msg is invalid!!!"<<endl;
		return;
	}
	cout << "接收到一个有效的 Commit 事件: txid=" << pe.m_lTxid<< endl;
	if(pe.m_lTxid>m_pPaxosState->m_iMaxCommitTxid)
	{
		m_pPaxosState->m_iMaxCommitTxid=pe.m_lTxid;
	}	
	cout<<endl<<endl;
}

