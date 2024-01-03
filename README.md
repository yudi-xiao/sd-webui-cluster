## Setup amqp url from codespace sercet

Under `https://github.com/{owner}/{repo}/settings/secrets/codespaces` add a codespaces secrets naming `AMQP_URL`

## Start up

```
streamlit run index.py --server.enableCORS false --server.enableXsrfProtection false --browser.gatherUsageStats false
```