import { MongoClient, OptionalId, Filter, Document, Db } from 'mongodb';
import { ed25519 } from '@noble/curves/ed25519';
import * as utils from '@noble/curves/abstract/utils';
import { PubSub } from './pub-sub';

function errorMessage(error: any) {
    return error.message ? error.message : 'Unknown error';
}

export class DbManager {
    private _client: MongoClient | null = null;
    private _db: Db | null = null;
    private _pubSub: PubSub = new PubSub();

    constructor() {
        this._pubSub.onReceive(this._onReceive);
    }

    async connect(dbAddress: string, selfAddress: string, peerAddresses: string[], maxConnections: number) {

        if (!await this._tryConnectToMongoDb(dbAddress))
            return;

        this._pubSub.connect(selfAddress, peerAddresses, maxConnections);
    }

    disconnect() {
        this._pubSub.disconnect();
        this._disconnectFromMongoDb();
    };

    private async _tryConnectToMongoDb(dbAddress: string) {
        const m = dbAddress.match(/(.*)\/(.*)\/?/);
        const uri = m?.at(1);
        const dbName = m?.at(2);

        if (!uri || !dbName) {
            console.log(`Invalid database address: ${dbAddress}`);
            return false;
        }

        try {
            this._client = new MongoClient(uri);
            await this._client.connect();
            this._db = this._client.db(dbName);

            console.log(`Successfully connected to database: ${dbAddress}`);
        }
        catch (error: any) {
            console.log(`Error while connecting to database '${dbAddress}': ${errorMessage(error)}`);
            return false;
        }

        return true;
    }

    private _disconnectFromMongoDb() {
        this._db = null;
        this._client?.close();
        this._client = null;
    }

    private _onReceive(obj: any) {
        switch (obj.action) {
            case 'insertOne':
                {
                    const { name, publicKey, entry } = obj;
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

        try {
            if (!ed25519.verify(
                entry._signature,
                utils.bytesToHex(new TextEncoder().encode(JSON.stringify({ ...entry, _signature: '' }))),
                publicKey)) {
                console.log(`Failed to verify entry signature '${entry._signature}' using owner pubic key '${publicKey}', for collection '${name}'`);
                return null;
            }
        }
        catch (error: any) {
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
                this._pubSub.sendToPeers({ action: 'insertOne', name, publicKeyOwner, entry });
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