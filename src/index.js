import express from 'express';

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.get('/', (_req, res) => {
  res.send('Welcome to <a href="https://www.stacksdrive.com">Stacks Drive</a>\'s hub!');
});

// Listen to the App Engine-specified port, or 8088 otherwise
const PORT = process.env.PORT || 8088;
app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}...`);
  console.log('Press Ctrl+C to quit.');
});
