"""Provides routines to describe an image using LLM-generated text."""

from pathlib import Path
import base64
import logging
import mimetypes
from os import environ as ENV
from typing import Any

from airflow.providers.amazon.aws.operators.bedrock import BedrockInvokeModelOperator
from botocore.exceptions import ClientError
from mokelumne.util import langfuse


logger = logging.getLogger(__name__)
"""The logger object used for any output of the image describer."""


BEDROCK_ANTHROPIC_VERSION: str = ENV.get("AWS_BEDROCK_ANTHROPIC_VERSION", "bedrock-2023-05-31")
"""Anthropic API version to use when invoking Bedrock models."""

BEDROCK_MAX_TOKENS: int = int(ENV.get("AWS_BEDROCK_MAX_TOKENS", "1024"))
"""Default token cap for Bedrock model responses."""


def _build_bedrock_input(prompt: str, encoded_image: str, mime_type: str) -> dict[str, Any]:
    """Build the Bedrock InvokeModel payload for Anthropic multimodal requests."""
    return {
        "anthropic_version": BEDROCK_ANTHROPIC_VERSION,
        "max_tokens": BEDROCK_MAX_TOKENS,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": mime_type,
                            "data": encoded_image,
                        },
                    },
                ],
            }
        ],
    }


def _extract_description(response: dict[str, Any]) -> str | None:
    """Extract generated text from Bedrock provider responses."""
    content = response.get("content")
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text" and block.get("text"):
                return str(block["text"])

    for key in ("generation", "outputText", "completion"):
        text = response.get(key)
        if isinstance(text, str) and text:
            return text

    return None


class BedrockModelAdapter:
    """Model adapter that lets ImageDescriber invoke Bedrock through Airflow's operator."""

    def __init__(self, context: dict[str, Any], model_id: str, aws_conn_id: str):
        self.context = context
        self.model_id = model_id
        self.aws_conn_id = aws_conn_id
        

    def describe_image(self, prompt: str, encoded_image: str, mime_type: str) -> str | None:
        """Invoke Bedrock for a single image description request and return extracted text."""
        operator_kwargs: dict[str, Any] = {
            "task_id": "invoke_bedrock_model",
            "model_id": self.model_id,
            "input_data": _build_bedrock_input(prompt, encoded_image, mime_type),
            "aws_conn_id": self.aws_conn_id,
        }

        response = BedrockInvokeModelOperator(**operator_kwargs).execute(self.context)

        return _extract_description(response)


class ImageDescriber:
    """Generates a description of a given image using a large language model."""

    def __init__(self, model, prompt: str, conn_id: str = 'langfuse_default'):
        """Initialise an image describer with a given model and prompt."""

        self.model = model
        self.prompt = prompt
        self.conn_id = conn_id
        self.langfuse_client = langfuse.get_langfuse_client(self.conn_id)

    def describe(self, record: dict[str, str]) -> dict[str, str]:
        """Describe an image described by a record, returning the output record.

        :param dict record: The record to process and describe.
        :returns dict: The record, as processed, with the `Status` set.
        If `Status` is `success`, then `Description` is also set to the generated description.
        """
        mime_type, _ = mimetypes.guess_type(record["Image Path"])
        if not mime_type:
            record["Status"] = "failure"
            record["Status description"] = "unable to determine image MIME type"
            return record

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
            record["Status"] = "failure"
            record["Status description"] = "file size exceeds limit"
            return record

        try:
            with self.langfuse_client.start_as_current_observation(
                as_type="generation",
                name="describe-image",
                model=getattr(self.model, "model_id", None),
                input={
                    "record_id": record["Record ID"],
                    "image_path": record["Image Path"],
                    "mime_type": mime_type,
                },
            ) as generation:
                description = self.model.describe_image(self.prompt, encoded, mime_type)
                if description:
                    generation.update(output=description)
                else:
                    generation.update(output="no content in response")
        except ClientError as exc:
            record["Status"] = "failure"
            record["Status description"] = exc.response["Error"]["Message"]
            return record

        if description:
            record["Status"] = "success"
            record["Description"] = description
        else:
            record["Status"] = "failure"
            record["Status description"] = "no content in response"

        return record
