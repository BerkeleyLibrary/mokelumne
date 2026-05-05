"""Provides routines to describe an image using LLM-generated text."""

from pathlib import Path
import base64
import logging
import mimetypes

from botocore.exceptions import ClientError
from langchain_core.messages import HumanMessage, SystemMessage
from langfuse.langchain import CallbackHandler


logger = logging.getLogger(__name__)
"""The logger object used for any output of the image describer."""


class ImageDescriber:
    """Generates a description of a given image using a large language model."""

    def __init__(self, model, prompt: str):
        """Initialise an image describer with a given model and prompt."""
        self.model = model
        self.prompt = prompt
        self.langfuse_handler = CallbackHandler()
        self.sys_msg = SystemMessage(prompt)

    def describe(self, record: dict[str, str]) -> dict[str, str]:
        """Describe an image described by a record, returning the output record.

        :param dict record: The record to process and describe.
        :returns dict: The record, as processed, with the `Status` set.
        If `Status` is `success`, then `Description` is also set to the generated description.
        """
        mime_type, _ = mimetypes.guess_type(record["Image Path"])
        encoded = base64.b64encode(Path(record["Image Path"]).read_bytes()).decode("utf-8")
        logger.info("Processing %s with file %s...", record["Record ID"], record["Image Path"])

        # we could make this a constant or env var.
        if len(encoded) > 3.75 * 1024 * 1024:
            record_meta = f"{record['Record ID']},{record['035__a']},{record['Image Path']}"
            logger.warning(
                "Encoded size %s exceeds limit for {%s}. Skipping record.",
                len(encoded),
                record_meta,
            )
            record["Status"] = "failure: file size exceeds limit"
            return record

        image_msg = HumanMessage(
            content=[{"type": "image", "base64": encoded, "mime_type": mime_type}]
        )

        try:
            result = self.model.invoke(
                [self.sys_msg, image_msg], config={"callbacks": [self.langfuse_handler]}
            )
        except ClientError as exc:
            record["Status"] = f"failure: {exc.response['Error']['Message']}"
            return record

        if hasattr(result, "content"):
            record["Status"] = "success"
            record["Description"] = str(result.content)
        else:
            record["Status"] = "failure: no content in response"

        return record
