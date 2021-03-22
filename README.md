# tap-postmark

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Postmark](https://postmarkapp.com/developer)
- Extracts the following resources:
  - [Messages opens](https://postmarkapp.com/developer/api/messages-api#message-opens)
  - [Messages outbound](https://postmarkapp.com/developer/api/messages-api#outbound-message-search)
  - [Stats outbound bounces](https://postmarkapp.com/developer/api/stats-api#bounce-counts)
  - [Stats outbound clients](https://postmarkapp.com/developer/api/stats-api#email-client-usage)
  - [Stats outbound overview](https://postmarkapp.com/developer/api/stats-api#overview)
  - [Stats outbound platform](https://postmarkapp.com/developer/api/stats-api#email-platform-usage)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

### Step 1: Create an API key

Create an API key in Postmark

### Step 2: Configure

Create a file called `postmark_config.json` in your working directory, following [sample_config.json](sample_config.json). The required parameters is the `postmark_server_token`.

This requires a `state.json` file to let the tap know from when to retrieve data. For example:
```
{
  "bookmarks": {
    "messages_opens": {
      "start_date": "2021-03-01"
    },
    "messages_outbound": {
      "start_date": "2021-03-01"
    },
    "stats_outbound_bounces": {
      "start_date": "2021-01-01"
    },
    "stats_outbound_clients": {
      "start_date": "2021-01-01"
    },
    "stats_outbound_overview": {
      "start_date": "2021-01-01"
    },
    "stats_outbound_platform": {
      "start_date": "2021-01-01"
    }
  }
}
```
Will replicate data from those dates. Please note that the messages endpoints start date can be at maximum 45 days in the past.

### Step 3: Install and Run

Create a virtual Python environment for this tap. This tap has been tested with Python 3.7, 3.8 and 3.9 and might run on future versions without problems.
```
python -m venv singer-postmark
singer-postmark/bin/python -m pip install --upgrade pip
singer-postmark/bin/pip install git+https://github.com/Yoast/singer-tap-postmark.git
```

This tap can be tested by piping the data to a local JSON target. For example:

Create a virtual Python environment with `singer-json`
```
python -m venv singer-json
singer-json/bin/python -m pip install --upgrade pip
singer-json/bin/pip install target-json
```

Test the tap:

```
singer-postmark/bin/tap-postmark --state state.json -c postmark_config.json | singer-json/bin/target-json >> state_result.json
```

Copyright &copy; 2021 Yoast