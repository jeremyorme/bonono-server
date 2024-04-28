import express from 'express';
import portfinder from 'portfinder';
import { DbManager } from './db-manager';
import { ed25519 } from '@noble/curves/ed25519';
import * as utils from '@noble/curves/abstract/utils';

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

const dbManager = new DbManager();

app.get('/', (req, res) => {
    res.send('This is a p2p database server!');
});

// Post one or more signed entries into a collection and return their ids
app.post('/collection/:name/:publicKey', async (req, res) => {
    const result = await dbManager.insertOne(req.params.name, req.params.publicKey, req.body);
    res.send(result);
});

// Get a range of entries from a collection
app.get('/collection/:name/:publicKey', async (req, res) => {
    const result = await dbManager.find(req.params.name, req.params.publicKey, req.body);
    res.send(result);
});

// Post one or more signed entries into a public collection and return their ids
app.post('/collection/:name', async (req, res) => {
    const result = await dbManager.insertOne(req.params.name, null, req.body);
    res.send(result);
});

// Get a range of entries from a public collection
app.get('/collection/:name', async (req, res) => {
    const result = await dbManager.find(req.params.name, null, req.body);
    res.send(result);
});

(async () => {
    const port = await portfinder.getPortPromise({ port: 3000 });
    app.listen(port, () => {
        console.log(`The http server is listening on port ${port}!`);

        const selfAddress = '127.0.0.1';
        const peerAddresses = [
            '127.0.0.1:5001',
            '127.0.0.1:5002',
            '127.0.0.1:5003',
            '127.0.0.1:5004',
            '127.0.0.1:5005',
            '127.0.0.1:5006',
            '127.0.0.1:5007'];
        const maxConnections = 2;
        dbManager.connect(`mongodb://127.0.0.1:27017/music-${port}`, selfAddress, peerAddresses, maxConnections);

        // Client code examples...

        // Generate a key-pair
        const priv = ed25519.utils.randomPrivateKey();
        const pub = ed25519.getPublicKey(priv);
        let keys = {
            privateKey: utils.bytesToHex(priv),
            publicKey: utils.bytesToHex(pub)
        };
        console.log(JSON.stringify(keys, undefined, 4));

        keys = {
            "privateKey": "1aaaab8a829e9861a3c5b0cb5a31d188ec7ef66611016c0755f217cb382ad0f9",
            "publicKey": "bc1574acbd07cd903918b9dbed20936dedde9a8a34551cf3e932de527c881a17"
        }

        // Create a signed entry
        const entry = {
            _entryId: 'bc1574acbd07cd903918b9dbed20936dedde9a8a34551cf3e932de527c881a17/0',
            _clock: 0,
            _signature: '',
            artist: 'Air',
            title: 'Moon Safari'
        };
        entry._signature = utils.bytesToHex(
            ed25519.sign(
                utils.bytesToHex(new TextEncoder().encode(JSON.stringify(entry))),
                keys.privateKey));

        console.log(JSON.stringify(entry, undefined, 4));
    });
})();

async function onTerminate() {
    await dbManager.disconnect();
    process.exit(0);
}

process.on('SIGTERM', onTerminate);
process.on('SIGINT', onTerminate);