# data.world & Amazon MWS Connector

[![Deploy](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy?template=https://github.com/datadotworld/heroku-dw-mws-connector)

## Getting Started

1. [Create a dataset](https://data.world/create-a-dataset) on data.world
2. Create a [Heroku](https://www.heroku.com) account
3. [Verify](https://heroku.com/verify) the account; basically, add a credit card
4. Deploy to Heroku using the fancy-looking button above (details on the configuration variables are available below)
5. Setup the Scheduler plugin
 
### Configuration Variables:

 * Your credentials for Amazon MWS are provided when you register as a developer. Instructions for doing so are available [here](http://docs.developer.amazonservices.com/en_US/dev_guide/DG_Registering.html)
 * `MARKETPLACE_IDS:` Marketplace IDs for the marketplaces you are registered to sell in. Potential values can be found [here](http://docs.developer.amazonservices.com/en_US/dev_guide/DG_Endpoints.html)
 