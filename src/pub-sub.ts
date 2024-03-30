import { Server, Socket, createConnection, createServer } from "net";
import * as portfinder from "portfinder"

enum PeerMessageType {
    PeerInfo,
    PeerData
}

interface PeerMessage {
    type: PeerMessageType
}

interface PeerInfoMessage extends PeerMessage {
    address: string;
    port: number;
}

interface PeerDataMessage extends PeerMessage {
    data: any;
}

export class PubSub {
    private _onReceive: (obj: any) => void = (obj: any) => { };
    private _server: Server | null = null;
    private _serverAddress: string = '';
    private _serverPort: number = 0;
    private _addressToSocket: Map<string, Socket> = new Map();
    private _socketToAddress: Map<Socket, string> = new Map();

    async connect(selfAddress: string, peerAddresses: string[], maxConnections: number) {
        this._serverAddress = selfAddress;
        await this._startListeningForPeers();
        this._startConnectingToPeers(peerAddresses, maxConnections);
    }

    onReceive(callback: (obj: any) => void) {
        this._onReceive = callback;
    }

    sendToPeers(obj: any) {
        const json = JSON.stringify(obj);
        const buffer = new TextEncoder().encode(json);
        for (const [_, socket] of this._addressToSocket) {
            socket.write(buffer);
        }
    }

    disconnect() {
        this._stopListeningForPeers();
    }

    private _isSelfAddress(host: string) {
        return host == "localhost" || host == "127.0.0.1" || host == this._serverAddress;
    }

    private async _startListeningForPeers() {

        if (this._server)
            return;

        this._serverPort = await portfinder.getPortPromise({ port: 5001 });

        this._server = createServer(socket => {
            // New provisional connection from you to me (pending peer info)
            socket.on('data', msg => {
                try {
                    const json = new TextDecoder().decode(msg);
                    const peerMsg = JSON.parse(json) as PeerMessage;
                    if (!peerMsg) {
                        console.log('Received invalid message from peer');
                        return;
                    }
                    this._onPeerMessage(peerMsg, socket);
                }
                catch (error: any) {
                    console.log(error);
                }
            });

            socket.on('end', () => {
                // Connection from you to me ended
                const address = this._socketToAddress.get(socket);
                if (address) {
                    this._onPeerDisconnected(address);
                    this._addressToSocket.delete(address);
                    this._socketToAddress.delete(socket);
                }
            });

            socket.on('error', console.log);
        });

        this._server.on('error', console.log);
        this._server.listen(this._serverPort, () => {
            this._onListening();
        });
    }

    private _stopListeningForPeers() {

        if (!this._server)
            return;

        for (const [_, socket] of this._addressToSocket) {
            socket.end();
        }
        this._server.close();

        this._addressToSocket = new Map();
        this._socketToAddress = new Map();
        this._serverPort = 0;
        this._server = null;
    }

    private _startConnectingToPeers(addresses: string[], maxConnections: number) {
        const maxCxns = Math.min(maxConnections, addresses.length / 2);

        if (this._addressToSocket.size < maxCxns) {
            const randomCxnIdx = Math.round(Math.random() * (addresses.length - 1));
            this._tryConnectToPeer(addresses[randomCxnIdx]);
        }

        setTimeout(() => this._startConnectingToPeers(addresses, maxConnections), 5000);
    }

    private _tryConnectToPeer(address: string) {
        if (this._addressToSocket.has(address)) {
            console.log(`Already connected to: ${address}`);
            return;
        }
        const [host, portStr] = address.split(':');
        const port = parseInt(portStr);

        if (Number.isNaN(port)) {
            console.log('Missing or invalid port number in peer address: ' + address);
            return;
        }

        if (this._isSelfAddress(host) && port == this._serverPort) {
            console.log('Skipping attempt to connect to self');
            return;
        }
        const socket = createConnection(port, host, () => {
            // New connection from me to you
            const peerInfo: PeerInfoMessage = {
                type: PeerMessageType.PeerInfo,
                address: this._serverAddress,
                port: this._serverPort
            };
            socket.write(new TextEncoder().encode(JSON.stringify(peerInfo)));
            this._onPeerConnected(address);
            this._addressToSocket.set(address, socket);
            this._socketToAddress.set(socket, address);
        });

        socket.on('close', () => {
            // Connection from me to you ended
            this._onPeerDisconnected(address);
            this._addressToSocket.delete(address);
            this._socketToAddress.delete(socket)
        });

        socket.on('data', msg => {
            try {
                const json = new TextDecoder().decode(msg);
                const obj = JSON.parse(json);
                this._onPeerReceived(obj)
            }
            catch (error: any) {
                console.log(error);
            }
        });

        socket.on('error', console.log);
    }

    private _onListening() {
        console.log(`Listening for peers on port ${this._serverPort}`);
    }

    private _onPeerMessage(peerMsg: PeerMessage, socket: Socket) {
        switch (peerMsg.type) {
            case PeerMessageType.PeerInfo:
                {
                    // Confirmed connection from you to me
                    const peerInfo = peerMsg as PeerInfoMessage;
                    const address = `${peerInfo.address}:${peerInfo.port}`
                    this._onPeerConnected(address);
                    this._addressToSocket.set(address, socket);
                    this._socketToAddress.set(socket, address);
                }
                break;
            case PeerMessageType.PeerData:
                {
                    // Data received
                    const peerData = peerMsg as PeerDataMessage;
                    this._onPeerReceived(peerData.data);
                }
                break;
        }
    }

    private _onPeerConnected(address: string) {
        console.log(`Peer connected: ${address}`);
    }

    private _onPeerDisconnected(address: string) {
        console.log(`Peer disconnected: ${address}`);
    }

    private _onPeerReceived(obj: any) {
        console.log('Received data');
        console.log(obj);
        this._onReceive(obj);
    }
}