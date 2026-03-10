"""
Proof of concept script for Mokelumne process:

1. Fetch TIND object by given ID.
2. Make LLM call with prompt and image.
3. Return a description.
"""

import base64
import logging
import os
import pathlib
import sys
import tempfile

from langchain.messages import HumanMessage, SystemMessage
from langchain_aws import ChatBedrock
from langfuse.langchain import CallbackHandler
from rich.console import Console
from tind_client import TINDClient

from mokelumne.util import langfuse


CONSOLE = Console()
"""The global console to use for user interaction."""


def _message(msg: str, level: int = logging.INFO) -> None:
    """Print a message to the `console`.

    :param Console console: The console on which to print the message.
    :param str msg: The message to print.
    :param int level: The level of the message: INFO, WARNING, or ERROR.
    """
    fmts = {logging.INFO: '[bold cyan]INFO:[/bold cyan]',
            logging.WARNING: '[bold yellow]WARNING:[/bold yellow]',
            logging.ERROR: '[bold red]ERROR:[/bold red]'}

    CONSOLE.print(f'{fmts[level]} {msg}')


def error_and_exit(msg: str) -> None:
    """Print an error to the console, and then exit with a failed status."""
    _message(msg, logging.ERROR)
    sys.exit(1)


def main() -> None:  # pylint: disable=R0914
    """The entry point for the script."""
    if len(sys.argv) > 2:
        error_and_exit('Invalid parameters.')

    if len(sys.argv) == 2:
        tind_id = sys.argv[-1]
    else:
        try:
            tind_id = CONSOLE.input('Enter the TIND ID to fetch: ')
        except EOFError:
            sys.exit(0)  # No data provided, user terminated.

    msg = f'Fetching metadata for {tind_id} from TIND...'
    with CONSOLE.status(msg):
        client = TINDClient()
        marc = client.fetch_marc_by_ids([tind_id])
        files = client.fetch_file_metadata(tind_id)
    CONSOLE.print(f':heavy_check_mark: {msg}')

    if len(marc) != 1:
        error_and_exit('More than one record was returned.')

    if len(files) != 1:
        error_and_exit('The MARC record must contain exactly one image.')

    if not files[0]['mime'].startswith('image'):
        error_and_exit('The MARC record must be for an image.')

    msg = 'Fetching prompt from Langfuse...'
    with CONSOLE.status(msg):
        prompt = langfuse.get_prompt()
        langfuse_handler = CallbackHandler()
    CONSOLE.print(f':heavy_check_mark: {msg}')

    msg = 'Fetching image from TIND...'
    with CONSOLE.status(msg):
        dl_url = files[0]['url']
        storage_dir = tempfile.mkdtemp(prefix='mokelumne')
        file_path = ''
        try:
            file_path = client.fetch_file(dl_url, storage_dir)
            with open(file_path, 'rb') as handle:
                image_content = base64.b64encode(handle.read()).decode('utf-8')
        except Exception as exc:  # pylint: disable=W0718
            error_and_exit(f'Failed to download {dl_url} to {storage_dir}: {exc}')
        finally:
            if file_path != '':
                pathlib.Path(file_path).unlink()
            pathlib.Path(storage_dir).rmdir()
    CONSOLE.print(f':heavy_check_mark: {msg}')

    model = ChatBedrock(model=os.environ['AWS_MODEL_ID'],
                        provider='anthropic')
    sys_msg = SystemMessage(prompt.prompt)
    image_msg = HumanMessage(content=[{'type': 'image',
                                       'base64': image_content,
                                       'mime_type': files[0]['mime']}])
    result = model.invoke([sys_msg, image_msg],
                          config={'callbacks': [langfuse_handler]})
    if hasattr(result, 'content'):
        CONSOLE.print(f"[bold green]Result:[/bold green] {result.content}")
    else:
        CONSOLE.print(result)


if __name__ == "__main__":
    main()
