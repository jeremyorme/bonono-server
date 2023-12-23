import { MongoClient, OptionalId, Filter, Document, Db } from 'mongodb';
import { ed25519 } from '@noble/curves/ed25519';
import * as utils from '@noble/curves/abstract/utils';
import * as net from 'net';
import portfinder from 'portfinder';

function errorMessage(error: any) {
    return error.message ? error.message : 'Unknown error';
}

export class DbManager
{
    private _db: Db | null = null;
    private _server: net.Server | null = null;
    private _serverPort: number = 0;
    private _clients: Map<string, net.Socket> = new Map();
        
    async connect(dbAddress: string, peerAddresses: string[]) {
        const m = dbAddress.match(/(.*)\/(.*)\/?/);
        const uri = m?.at(1);
        const dbName = m?.at(2);

        if (!uri || !dbName) {
            console.log(`Invalid database address: ${dbAddress}`);
            return;
        }

        try {
            const client = new MongoClient(uri);
            await client.connect();
            this._db = client.db(dbName);

            console.log(`Successfully connected to database: ${dbAddress}`);
        }
        catch (error: any) {
            console.log(`Error while connecting to database '${dbAddress}': ${errorMessage(error)}`);
            return;
        }
        
        this._serverPort = await portfinder.getPortPromise({port: 5000});

        this._server = net.createServer(socket => {
            this._onPeerConnected(socket);

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

            socket.on('end', () => {
                this._onPeerDisconnected(socket);
            });

            socket.on('error', console.log);
        });

        this._server.on('error', console.log);
        this._server.listen(this._serverPort, () => {
            this._onListening();
        });

        this._tryConnectToPeers(peerAddresses);
    }

    async disconnect() {
        for (const [_, socket] of this._clients) {
            socket.end();
        }
        this._server?.close();
    };

    private _tryConnectToPeers(addresses: string[]) {
        if (this._clients.size < addresses.length)
        {
            for (const address of addresses) {
                this._tryConnectToPeer(address);
            }
        }

        setTimeout(() => this._tryConnectToPeers(addresses), 5000);
    }

    private _tryConnectToPeer(address: string) {
        if (this._clients.has(address)) {
            console.log(`Already connected to: ${address}`);
            return;
        }
        const [host, portStr] = address.split(':');
        const port = parseInt(portStr);
        if (host == 'localhost' && port == this._serverPort) {
            console.log('Skipping attempt to connect to self');
            return;
        }
        const socket = net.createConnection(port, host, () => {
            this._onPeerConnected(socket);
            this._clients.set(address, socket);
        });

        socket.on('close', () => {
            this._onPeerDisconnected(socket);
            this._clients.delete(address);
        });

        socket.on('error', console.log);
    }

    private _sendToPeers(obj: any) {
        const json = JSON.stringify(obj);
        const buffer = new TextEncoder().encode(json);
        for (const [_, socket] of this._clients) {
            socket.write(buffer);
        }
    }

    private _onListening() {
        console.log(`Listening for peers on port ${this._serverPort}`);
    }

    private _onPeerConnected(socket: net.Socket) {
        console.log(`Peer connected on port ${socket.localPort}/${socket.remotePort}`);
    }

    private _onPeerDisconnected(socket: net.Socket) {
        console.log('Peer disconnected');
    }

    private _onPeerReceived(obj: any) {
        console.log('Received data');
        console.log(obj);

        switch (obj.action) {
            case 'insertOne':
                {
                    const {name, publicKey, entry} = obj;
                    this.insertOne(name, publicKey, entry, false);
                }
                break;
        }
    }

    // Insert single entry into collection with specified address
    async insertOne(name: string, publicKeyOwner: string | null, entry: OptionalId<Document>, notifyPeers: boolean = true) {
        if (!this._db) {
            console.log(`Attempt to insert entry into collection '${name}' before connection`);
            return null;
        }

        if (!entry._id && entry._id != 0) {
            console.log(`Attempt to insert entry into collection '${name}' without _id`);
            return null;
        }

        const id = entry._id.toString();
        const publicKey = publicKeyOwner || (id.match(/^[0-9a-f]+\//) ? id.split('/')[0] : null);
        if (!publicKey) {
            console.log(`Attempt to insert entry into public collection '${name}' without public key prefix on _id`);
            return null;
        }

        try
        {
            if (!ed25519.verify(
                entry._signature,
                utils.bytesToHex(new TextEncoder().encode(JSON.stringify({...entry, _signature: ''}))),
                publicKey)) {
                console.log(`Failed to verify entry signature '${entry._signature}' using owner pubic key '${publicKey}', for collection '${name}'`);
                return null;
            }
        }
        catch (error: any)
        {
            console.log(`Exception while verifying entry signature '${entry._signature}' using owner pubic key '${publicKey}', for collection '${name}': ${errorMessage(error)}`);
            return null;
        }

        const address = publicKeyOwner ? `${name}/${publicKeyOwner}` : name;

        try {
            const col = this._db.collection(address);
        
            console.log(`Successfully opened collection '${col.collectionName}'`);

            const result = await col.insertOne(entry);

            if (!result) {
                console.log('Failed to insert entry into the collection');
                return null;
            }

            console.log(`Successfully created a new entry in collection '${address}' with id ${result.insertedId}`);

            if (notifyPeers) {
                this._sendToPeers({action: 'insertOne', name, publicKeyOwner, entry});
            }

            return result;
        }
        catch (error: any) {
            console.log(`Error while writing to collection '${address}': ${errorMessage(error)}`);
            return null;
        }
    }

    // Find documents matching filter criteria
    async find(name: string, publicKeyOwner: string | null, filter: Filter<Document>) {
        if (!this._db) {
            console.log(`Attempt to find entries in collection '${name}' before connection`);
            return null;
        }

        const address = publicKeyOwner ? `${name}/${publicKeyOwner}` : name;

        try {
            const col = this._db.collection(address);

            console.log(`Successfully opened collection '${col.collectionName}'`);

            const result = col.find(filter);

            console.log(`Successfully found results`);

            return result;
        }
        catch (error: any) {
            console.log(`Error while querying collection '${address}': ${errorMessage(error)}`);
            return null;
        }
    }
}