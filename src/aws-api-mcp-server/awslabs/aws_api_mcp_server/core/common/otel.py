# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import time
from contextlib import contextmanager
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import Span as SDKSpan


logger = logging.getLogger(__name__)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import Status, StatusCode
from typing import Any, Dict, List, Optional


# AWS X ray support via OTLP
XRAY_OTLP_ENDPOINT = 'http://localhost:2000/v1/traces'

try:
    from aws_xray_sdk.core import xray_recorder
    from aws_xray_sdk.core.context import Context as XRayContext

    XRAY_SDK_AVAILABLE = True

    # Configure X-Ray recorder properly
    xray_recorder.configure(
        service='aws-api-mcp-server',
        context_missing='LOG_ERROR',
    )
except ImportError:
    XRAY_SDK_AVAILABLE = False


class SpanCollector:
    def __init__(self):
        self.spans: List[SDKSpan] = []

    def add_span(self, span: SDKSpan):
        self.spans.append(span)

    def clear(self):
        self.spans.clear()


class OTelTraceManager:

    def __init__(self):
        logger.info('=== OTEL TRACE MANAGER INIT ===')
        logger.info(f'XRAY_SDK_AVAILABLE: {XRAY_SDK_AVAILABLE}')
        logger.info(
            f'AWS_XRAY_TRACING_ENABLED: {os.getenv("AWS_XRAY_TRACING_ENABLED", "NOT_SET")}'
        )
        logger.info(
            f'AWS_API_MCP_ENABLE_OTEL_TRACES: {os.getenv("AWS_API_MCP_ENABLE_OTEL_TRACES", "NOT_SET")}'
        )
        logger.info('=== END INIT DEBUG ===')

        self.tracer_provider = TracerProvider()

        console_exporter = ConsoleSpanExporter()
        self.tracer_provider.add_span_processor(BatchSpanProcessor(console_exporter))

        xray_enabled = os.getenv('AWS_XRAY_TRACING_ENABLED', 'false').lower() == 'true'

        if not xray_enabled:
            otlp_endpoint = os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT', 'http://localhost:4317')
            if otlp_endpoint:
                try:
                    otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
                    self.tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
                except Exception:
                    pass 

        trace.set_tracer_provider(self.tracer_provider)
        self.tracer = trace.get_tracer(__name__)
        self.span_collector = SpanCollector()

    def create_span(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> trace.Span:
        span = self.tracer.start_span(name)
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, str(value))
        return span

    @contextmanager
    def trace_operation(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        logger.info(f'=== TRACE OPERATION START: {name} ===')
        logger.info(f'XRAY_SDK_AVAILABLE: {XRAY_SDK_AVAILABLE}')
        logger.info(f'AWS_XRAY_TRACING_ENABLED: {os.getenv("AWS_XRAY_TRACING_ENABLED")}')

        span = self.create_span(name, attributes)

        if XRAY_SDK_AVAILABLE and name in ['aws_cli_command']:
            xray_enabled = os.getenv('AWS_XRAY_TRACING_ENABLED', 'false').lower() == 'true'
            logger.info(f'X-Ray enabled check: {xray_enabled}')

            if xray_enabled:
                logger.info(f'Using X-Ray context manager for: {name}')
                with xray_recorder.in_segment(name) as segment:
                    logger.info(f'X-Ray segment type: {type(segment)}')
                    logger.info(f'X-Ray segment ID: {getattr(segment, "id", "NO_ID")}')

                    segment.put_annotation('operation', name)
                    segment.put_annotation('service', 'aws-api-mcp-server')

                    if attributes:
                        for key, value in attributes.items():
                            clean_key = key.replace('.', '_').replace('-', '_')
                            if len(str(value)) <= 50:
                                segment.put_annotation(clean_key, str(value))
                            else:
                                segment.put_metadata(clean_key, str(value))

                    logger.info('X-Ray segment configured successfully')

                    try:
                        yield span
                    except Exception as e:
                        logger.error(f'Exception in trace operation: {e}')
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                        segment.add_exception(e)
                        raise
                    finally:
                        span.end()
                        if isinstance(span, SDKSpan):
                            self.span_collector.add_span(span)
                        logger.info('X-Ray segment will be automatically sent')
            else:
                logger.info('X-Ray disabled via environment variable')
                try:
                    yield span
                except Exception as e:
                    logger.error(f'Exception in trace operation: {e}')
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    raise
                finally:
                    span.end()
                    if isinstance(span, SDKSpan):
                        self.span_collector.add_span(span)
        else:
            logger.info(f'Skipping X-Ray segment creation for nested operation: {name}')
            try:
                yield span
            except Exception as e:
                logger.error(f'Exception in trace operation: {e}')
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise
            finally:
                span.end()
                if isinstance(span, SDKSpan):
                    self.span_collector.add_span(span)

        logger.info(f'=== TRACE OPERATION END: {name} ===')

    def add_result_to_current_segment(self, result_data: Dict[str, Any]):
        if XRAY_SDK_AVAILABLE:
            try:
                current_segment = xray_recorder.current_segment()
                if current_segment:
                    current_segment.put_metadata('output', result_data)
                    if 'exit_code' in result_data:
                        current_segment.put_annotation('exit_code', str(result_data['exit_code']))
                    if 'command' in result_data:
                        cmd = str(result_data['command'])
                        current_segment.put_annotation(
                            'command', cmd[:50]
                        )  # Truncate for annotation
                        current_segment.put_metadata('full_command', cmd)
            except Exception:
                pass

    def span_to_otlp_json(self, span: SDKSpan) -> Dict[str, Any]:
        span_context = span.get_span_context()

        return {
            'traceId': format(span_context.trace_id, '032x'),
            'spanId': format(span_context.span_id, '016x'),
            'name': span.name,
            'kind': 'SPAN_KIND_INTERNAL',
            'startTimeUnixNano': str(span.start_time),
            'endTimeUnixNano': str(span.end_time or time.time_ns()),
            'attributes': [
                {'key': k, 'value': {'stringValue': str(v)}}
                for k, v in (span.attributes or {}).items()
            ],
            'status': {
                'code': 'STATUS_CODE_OK'
                if span.status.status_code == StatusCode.OK
                else 'STATUS_CODE_ERROR'
            },
        }

    def get_resource_spans(self) -> Dict[str, Any]:
        spans_json = [self.span_to_otlp_json(span) for span in self.span_collector.spans]

        result = {
            'resource': {
                'attributes': [
                    {'key': 'service.name', 'value': {'stringValue': 'aws-api-mcp-server'}},
                    {'key': 'service.version', 'value': {'stringValue': '0.2.14'}},
                ]
            },
            'scopeSpans': [
                {'scope': {'name': 'aws-api-mcp-server', 'version': '0.2.14'}, 'spans': spans_json}
            ],
        }

        self.span_collector.clear()
        return result


otel_manager = OTelTraceManager()
