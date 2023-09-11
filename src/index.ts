import express from 'express';

const app = express();

app.get('/', (req, res) => {
    res.send('This is a bonono database server!');
});

// Post a collection manifest and return its address
app.post('/collection-manifest', (req, res) => {
    res.send('/collection-manifest');
});

// Get the collection manifests known to the server
app.get('/collection-manifest', (req, res) => {
    res.send('/collection-manifest');
});

// Get the collection manifest at the specified address
app.get('/collection-manifest/:address', (req, res) => {
    res.send('/collection-manifest/:address');
});

// Post one or more entries into a collection and return their ids
app.post('/collection/:address', (req, res) => {
    res.send('/collection/:address');
});

// Get one or more entries from a collection
app.get('/collection/:address', (req, res) => {
    res.send('/collection/:address');
});

app.listen(3000, () => {
    console.log('The application is listening on port 3000!');
});