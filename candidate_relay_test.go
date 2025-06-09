// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

//go:build !js
// +build !js

package ice

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pion/stun"
	"github.com/pion/transport/v2/test"
	"github.com/pion/turn/v2"
	"github.com/stretchr/testify/assert"
)

func optimisticAuthHandler(string, string, net.Addr) (key []byte, ok bool) {
	return turn.GenerateAuthKey("username", "pion.ly", "password"), true
}

func passiveTCPRelayGatherAndExchangeCandidates(agentWithRelay, agentNoRelay *Agent) {
	var wg sync.WaitGroup
	wg.Add(1)

	check(agentWithRelay.OnCandidate(func(candidate Candidate) {
		if candidate == nil {
			wg.Done()
		}
	}))

	// This gather will create a single passive TCP relay candidate.
	check(agentWithRelay.GatherCandidates())

	var tcpCandidateCreated sync.WaitGroup
	tcpCandidateCreated.Add(1)
	check(agentNoRelay.OnCandidate(func(candidate Candidate) {
		if candidate == nil {
			wg.Done()
		} else {
			if candidate.TCPType() != TCPTypeUnspecified {
				ip := net.ParseIP(candidate.Address()).To4()
				fmt.Println("Parsing:", candidate.Address(), "IP:", ip)
				fmt.Println("Doneing:", candidate.Marshal(), "Private?", ip.IsPrivate(), "ip[0]", int(ip[0]))
				if !ip.IsPrivate() && ip[0] != 100 {
					tcpCandidateCreated.Done()
				}
			}
		}
	}))

	// Wait for `agentWithRelay` to gather its candidate. We do not need to gather candidates for
	// `agentNoRelay`. It will create a candidate in response to receiving a passive TCP candidate.
	wg.Wait()

	// Communicate the relay candidate to `agentNoRelay`.
	relayCandidates, err := agentWithRelay.GetLocalCandidates()
	check(err)
	for _, relayCandidate := range relayCandidates {
		candidateCopy, copyErr := relayCandidate.copy()
		check(copyErr)
		check(agentNoRelay.AddRemoteCandidate(candidateCopy))
	}

	// Wait for the TCP candidate to be generated.
	tcpCandidateCreated.Wait()

	// Assert there's a single tcp candidate. And add it as a remote candidate for `agentWithRelay`.
	tcpCandidates, err := agentNoRelay.GetLocalCandidates()
	check(err)
	if len(tcpCandidates) < 1 {
		check(fmt.Errorf("Expected at least 1 TCP candidate. Found: %d", len(tcpCandidates)))
	}

	for _, c := range tcpCandidates {
		candidateCopy, copyErr := c.copy()
		check(copyErr)
		check(agentWithRelay.AddRemoteCandidate(candidateCopy))
	}
}

func passiveTCPRelayConnect(aAgent, bAgent *Agent) (*Conn, *Conn) {
	passiveTCPRelayGatherAndExchangeCandidates(aAgent, bAgent)

	accepted := make(chan struct{})
	var aConn *Conn

	go func() {
		var acceptErr error
		bUfrag, bPwd, acceptErr := bAgent.GetLocalUserCredentials()
		check(acceptErr)
		aConn, acceptErr = aAgent.Accept(context.TODO(), bUfrag, bPwd)
		check(acceptErr)
		close(accepted)
	}()
	aUfrag, aPwd, err := aAgent.GetLocalUserCredentials()
	check(err)
	bConn, err := bAgent.Dial(context.TODO(), aUfrag, aPwd)
	check(err)

	// Ensure accepted
	<-accepted
	return aConn, bConn
}

func TestRelayTCPConnection(t *testing.T) {
	// Standup coturn:
	//  /bin/turnserver -v -lt-cred-mech -u dan:dan -r pion.ly --allow-loopback-peers --cli-password=pw
	//
	// We create an ICE Agent that will create a single passive TCP relay candidate. A TURN server
	// that supports TCP allocations must be running locally on port 3478.
	cfgWithTURN := &AgentConfig{
		NetworkTypes: []NetworkType{NetworkTypeTCP4},
		Urls: []*stun.URI{
			{
				Scheme: stun.SchemeTypeTURN,

				Host:     "34.9.65.195",
				Username: "calamity",
				Password: "mD2nNWk9uAguCnWUffUP",

				// Host:     "127.0.0.1",
				// Username: "dan",
				// Password: "dan",

				Port:  3478,
				Proto: stun.ProtoTypeTCP,
				// Proto: stun.ProtoTypeUDP,
			},
		},
		CandidateTypes: []CandidateType{CandidateTypeRelay},

		// Without this, ICE will create a TCP connection to the TURN server (as per the `Proto:
		// ProtoTypeTCP`), but ask for a UDP allocation.
		UseTCPAllocationsForLocalRelayCandidates: true,
	}

	agentWithTURN, err := NewAgent(cfgWithTURN)
	if err != nil {
		t.Fatal(err)
	}

	// Create a channel that's closed when `agentWithRelay` goes into a connected state.
	aNotifier, aConnected := onConnected()
	if err = agentWithTURN.OnConnectionStateChange(aNotifier); err != nil {
		t.Fatal(err)
	}

	// Create a second agent that is capable of creating TCP candidates. The candidates will be
	// generated in response to a passive TCP remote candidate.
	cfgNoTURN := &AgentConfig{
		NetworkTypes: []NetworkType{NetworkTypeTCP4},
		Urls: []*stun.URI{
			{
				Scheme: stun.SchemeTypeSTUN,
				Host:   "34.9.65.195",
				Port:   3478,
				Proto:  stun.ProtoTypeUDP,
			},
		},

		// Explicitly demonstrate we do not need to generate any local candidates at the gathering
		// step.
		CandidateTypes: []CandidateType{},
	}

	agentNoTURN, err := NewAgent(cfgNoTURN)
	if err != nil {
		t.Fatal(err)
	}

	// Create a channel that's closed when `agentNoTURN` goes into a connected state.
	bNotifier, bConnected := onConnected()
	if err = agentNoTURN.OnConnectionStateChange(bNotifier); err != nil {
		t.Fatal(err)
	}

	// Kick off the agents and perform signaling/answering.
	passiveTCPRelayConnect(agentWithTURN, agentNoTURN)

	<-aConnected
	<-bConnected

	assert.NoError(t, agentWithTURN.Close())
	assert.NoError(t, agentNoTURN.Close())
}

func TestRelayOnlyConnection(t *testing.T) {
	// Limit runtime in case of deadlocks
	lim := test.TimeOut(time.Second * 30)
	defer lim.Stop()

	report := test.CheckRoutines(t)
	defer report()

	serverPort := randomPort(t)
	serverListener, err := net.ListenPacket("udp", "127.0.0.1:"+strconv.Itoa(serverPort))
	assert.NoError(t, err)

	server, err := turn.NewServer(turn.ServerConfig{
		Realm:       "pion.ly",
		AuthHandler: optimisticAuthHandler,
		PacketConnConfigs: []turn.PacketConnConfig{
			{
				PacketConn:            serverListener,
				RelayAddressGenerator: &turn.RelayAddressGeneratorNone{Address: "127.0.0.1"},
			},
		},
	})
	assert.NoError(t, err)

	cfg := &AgentConfig{
		NetworkTypes: supportedNetworkTypes(),
		Urls: []*stun.URI{
			{
				Scheme:   stun.SchemeTypeTURN,
				Host:     "127.0.0.1",
				Username: "username",
				Password: "password",
				Port:     serverPort,
				Proto:    stun.ProtoTypeUDP,
			},
		},
		CandidateTypes: []CandidateType{CandidateTypeRelay},
	}

	aAgent, err := NewAgent(cfg)
	if err != nil {
		t.Fatal(err)
	}

	aNotifier, aConnected := onConnected()
	if err = aAgent.OnConnectionStateChange(aNotifier); err != nil {
		t.Fatal(err)
	}

	bAgent, err := NewAgent(cfg)
	if err != nil {
		t.Fatal(err)
	}

	bNotifier, bConnected := onConnected()
	if err = bAgent.OnConnectionStateChange(bNotifier); err != nil {
		t.Fatal(err)
	}

	connect(aAgent, bAgent)
	<-aConnected
	<-bConnected

	assert.NoError(t, aAgent.Close())
	assert.NoError(t, bAgent.Close())
	assert.NoError(t, server.Close())
}
