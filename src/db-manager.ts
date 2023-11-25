import { MongoClient, OptionalId, Filter, Document, Db } from 'mongodb';
import { ed25519 } from '@noble/curves/ed25519';
import * as utils from '@noble/curves/abstract/utils';
import { Libp2p, createLibp2p } from 'libp2p';
import { noise } from '@chainsafe/libp2p-noise';
import { yamux } from '@chainsafe/libp2p-yamux';
import { gossipsub } from '@chainsafe/libp2p-gossipsub';
import { mdns } from '@libp2p/mdns';
import { tcp } from '@libp2p/tcp';
import { kadDHT } from '@libp2p/kad-dht';

function errorMessage(error: any) {
    return error.message ? error.message : 'Unknown error';
}

export class DbManager
{
    private _db: Db | null = null;
    private _libp2p: Libp2p | null = null;
    private _publish: (msg: any) => void = (msg: any) => console.log('Failed to publish: ', msg);
        
    async connect(dbAddress: string) {
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

        try {
            const libp2p = await createLibp2p({
                addresses: { listen: ['/ip4/127.0.0.1/tcp/0'] },
                transports: [tcp()],
                connectionEncryption: [noise()],
                streamMuxers: [yamux()],
                peerDiscovery: [mdns()],
                // peerDiscovery: [mdns(), bootstrap({list:[
                //     '/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb',
                //     '/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN']})],
                services: { dht: kadDHT(), pubsub: gossipsub() }
            });
            this._libp2p = libp2p;

            libp2p.addEventListener('peer:discovery', (peerInfo: any) => {
                console.log(`Found peer: ${peerInfo.detail.id}`);
            });

            libp2p.addEventListener('peer:connect', (peerId: any) => {
                console.log(`Connected to: ${peerId.detail}`);
            });

            libp2p.services.pubsub.addEventListener('message', (message: any) => {
                console.log(`${message.detail.topic}: `, new TextDecoder().decode(message.detail.data));
            });
        
            console.log('Listening on addresses:');
            libp2p.getMultiaddrs().forEach((addr) => {
                console.log(addr.toString());
            });
              
            libp2p.services.pubsub.subscribe('fruit');
            
            this._publish = (msg: any) => {
                libp2p.services.pubsub.publish('fruit', new TextEncoder().encode(msg));
                console.log('Published message: ', msg);
            }
        }
        catch (error: any) {
            console.error(error);
        }
    }

    async disconnect() {
        if (!this._libp2p) {
            console.log('Attempt to disconnect before connection');
            return;
        }
        await this._libp2p.stop();
        console.log('The libp2p node has stopped');
    };

    // Insert single entry into collection with specified address
    async insertOne(name: string, publicKey: string, entry: OptionalId<Document>) {
        if (!this._db) {
            console.log(`Attempt to insert entry into collection '${name}' before connection`);
            return null;
        }

        try
        {
            if (!ed25519.verify(
                entry._signature,
                utils.bytesToHex(new TextEncoder().encode(JSON.stringify({...entry, _signature: ''}))),
                publicKey)) {
                console.log(`Attempt to insert entry with signature that does not match the owner pubic key, into collection '${name}'`);
                return null;
            }
        }
        catch (error: any)
        {
            console.log(`Attempt to insert entry into collection '${name}' failed signature verification: ${errorMessage(error)}`);
            return null;
        }

        const address = `${name}/${publicKey}`;

        try {
            const col = this._db.collection(address);

            col.createIndex({ _clock: 1 });
            const last_entry = (await col.find().sort({ _clock: -1 }).limit(1).toArray())[0];
            const next_clock = last_entry ? last_entry._clock + 1 : 0;
            if (entry._clock != next_clock) {
                console.log(`Attempt to insert entry with invalid clock into collection '${name}'`);
                return null;
            }
        
            console.log(`Successfully opened collection '${col.collectionName}'`);

            const result = await col.insertOne(entry);

            if (!result) {
                console.log('Failed to insert entry into the collection');
                return null;
            }

            console.log(`Successfully created a new entry in collection '${address}' with id ${result.insertedId}`);

            this._publish('banana');

            return result;
        }
        catch (error: any) {
            console.log(`Error while writing to collection '${address}': ${errorMessage(error)}`);
            return null;
        }
    }

    // Find documents matching filter criteria
    async find(name: string, publicKey: string, filter: Filter<Document>) {
        if (!this._db) {
            console.log(`Attempt to find entries in collection '${name}' before connection`);
            return null;
        }

        const address = `${name}:${publicKey}`;

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