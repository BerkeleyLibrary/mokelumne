# Proof of concept script

This directory contains the *proof of concept* script for the batch image
description project (Mokelumne).  This script is not intended to be used
in a production environment.

## Environment variables

<dl>
<dt><code>TIND_API_KEY</code></dt>
<dd>The TIND API key to use for connecting to TIND.</dd>

<dt><code>TIND_API_URL</code></dt>
<dd>The TIND API server to connect to; you probably want
<code>https://digicoll.lib.berkeley.edu/api/v1</code>.</dd>

<dt><code>LANGFUSE_HOST</code></dt>
<dd>The Langfuse Cloud host to use; you almost definitely
want <code>https://us.cloud.langfuse.com</code>.</dd>

<dt><code>LANGFUSE_SECRET_KEY</code></dt>
<dd>The Langfuse secret key, found in credential storage.</dd>

<dt><code>LANGFUSE_PUBLIC_KEY</code></dt>
<dd>The Langfuse public key, found in credential storage.</dd>

<dt><code>AWS_ENDPOINT_URL</code></dt>
<dd>The AWS endpoint to use.</dd>

<dt><code>AWS_DEFAULT_REGION</code></dt>
<dd>The AWS region to use; you probably want <code>us-west-1</code>.</dd>

<dt><code>AWS_BEARER_TOKEN_BEDROCK</code></dt>
<dd>The IAM credential to use to access AWS.  Use a short-term API key.
The key will expire after AWS console logout or 12 hours (whichever comes first).</dd>

<dt><code>AWS_MODEL_ID</code></dt>
<dd>The model to invoke.  To use Claude Sonnet 4.5, you need an ARN that has
an inference model containing it.</dd>
</dl>

## Usage

Ensure the top-level package (`mokelumne`) is installed from the parent
directory of this directory (i.e., the Git root)::

```shell
pip install -e .
```

Call the script with a single argument, which should be the desired TIND ID:

```shell
python3 script.py 123456
```

where `123456` is the TIND ID.

You may need to specify `PYTHONPATH=..` depending on whether you `venv` or
`uv` and the current phase of the moon.

The script will output the image description generated on success, or a
detailed message describing any issues encountered while generating it.

For specifying credentials, I've been using a subshell.

Given `.env` contains:

```shell
export TIND_API_KEY=...
export TIND_API_URL=...
...
```

this can be done somewhat like:

```shell
(source .env; python3 script.py 123456)
```

which ensures none of the environment variables leak into your normal shell.
