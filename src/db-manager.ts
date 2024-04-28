import { MongoClient, OptionalId, Filter, Document, Db, ObjectId, Collection } from 'mongodb';
import { ed25519 } from '@noble/curves/ed25519';
import * as utils from '@noble/curves/abstract/utils';
import { PubSub } from './pub-sub';

enum DbMessageType {
    InsertOne
}

interface DbMessage {
    action: DbMessageType;
    excludeAddresses: string[];
}

interface DbMessageInsertOne extends DbMessage {
    action: DbMessageType.InsertOne,
    name: string;
    publicKeyOwner: string | null;
    entry: any;
}

function errorMessage(error: any) {
    return error.message ? error.message : 'Unknown error';
}

export class DbManager {
    private _client: MongoClient | null = null;
    private _db: Db | null = null;
    private _pubSub: PubSub = new PubSub();

    constructor() {
        this._pubSub.onReceive(obj => this._onReceive(obj));
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
        const dbMsg = obj as DbMessage;
        switch (dbMsg.action) {
            case DbMessageType.InsertOne:
                {
                    const insertMsg = dbMsg as DbMessageInsertOne;
                    this.insertOne(insertMsg.name, insertMsg.publicKeyOwner, insertMsg.entry, insertMsg.excludeAddresses);
                }
                break;
        }
    }

    // Insert single entry into collection with specified address
    async insertOne(name: string, publicKeyOwner: string | null, entry: OptionalId<Document>, excludeAddresses: string[] = []) {
        // Check DB init
        if (!this._db) {
            console.log(`Attempt to insert entry into collection '${name}' before connection`);
            return null;
        }

        // Check entry has an id
        if (!entry._entryId && entry._entryId != 0) {
            console.log(`Attempt to insert entry into collection '${name}' without _entryId`);
            return null;
        }

        // Check either collection or entry specifies the public key
        const entryId = entry._entryId.toString();
        const publicKey = publicKeyOwner || (entryId.match(/^[0-9a-f]+\//) ? entryId.split('/')[0] : null);
        if (!publicKey) {
            console.log(`Attempt to insert entry into public collection '${name}' without public key prefix on _id`);
            return null;
        }

        // Validate signature
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

        // Find collection
        const address = publicKeyOwner ? `${name}/${publicKeyOwner}` : name;
        var col: Collection<Document>;
        try {
            col = this._db.collection(address);
            console.log(`Successfully opened collection '${col.collectionName}'`);
            col.createIndex({ _entryId: 1 }, { unique: true, background: false });
        }
        catch {
            console.log(`Failed to create index for collection ${address}`);
            return null;
        }

        // Insert and send update to peers
        try {
            const originalEntry = { ...entry };
            const result = await col.insertOne(entry);

            if (!result) {
                console.log(`Skipping insert of already-seen entry '${entryId}'`);
                return null;
            }

            console.log(`Successfully created a new entry in collection '${address}' with id ${result.insertedId}`);

            const excludeAddressesNext = Array.from(new Set(excludeAddresses.length > 0 ?
                [...excludeAddresses, ...this._pubSub.cxnAddresses()] :
                [this._pubSub.selfAddress(), ...this._pubSub.cxnAddresses()]));

            const insertMsg: DbMessageInsertOne = {
                action: DbMessageType.InsertOne,
                excludeAddresses: excludeAddressesNext,
                name,
                publicKeyOwner,
                entry: originalEntry
            }

            this._pubSub.sendToPeers(insertMsg, excludeAddresses);

            return result;
        }
        catch (error: any) {
            console.log(`Error while writing to collection '${address}': ${errorMessage(error)}`);
            return null;
        }
    }

    // Find documents matching filter criteria
    async find(name: string, publicKeyOwner: string | null, filter: Filter<Document>) {
        // Check DB init
        if (!this._db) {
            console.log(`Attempt to find entries in collection '${name}' before connection`);
            return null;
        }

        // Find collection
        const address = publicKeyOwner ? `${name}/${publicKeyOwner}` : name;
        const col = this._db.collection(address);
        console.log(`Successfully opened collection '${col.collectionName}'`);

        // Query and return results
        try {
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