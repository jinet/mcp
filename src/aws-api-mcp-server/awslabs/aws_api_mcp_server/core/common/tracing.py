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

import os
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from functools import wraps
from loguru import logger
from typing import Any, Dict


class TraceBackend(ABC):
    
    @abstractmethod
    @contextmanager
    def create_span(self, name: str, attributes: Dict[str, Any]):
        pass

    @abstractmethod
    def get_resource_spans(self) -> Dict[str, Any]:
        pass


class NoOpBackend(TraceBackend):

    @contextmanager
    def create_span(self, name: str, attributes: Dict[str, Any]):
        yield None

    def get_resource_spans(self) -> Dict[str, Any]:
        return {'resource': {}, 'scopeSpans': []}


class OTLPBackend(TraceBackend):
    
    def __init__(self):
        self.spans = []
        try:
            from opentelemetry import trace
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

            self.tracer_provider = TracerProvider()
            self.tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

           
            endpoint = os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT', 'http://localhost:4317')
            try:
                otlp_exporter = OTLPSpanExporter(endpoint=endpoint)
                self.tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
            except Exception:
                pass

            trace.set_tracer_provider(self.tracer_provider)
            self.tracer = trace.get_tracer(__name__)
        except ImportError:
            logger.warning('OpenTelemetry not available, using no-op backend')
            self.tracer = None

    @contextmanager
    def create_span(self, name: str, attributes: Dict[str, Any]):
        if not self.tracer:
            yield None
            return

        span = self.tracer.start_span(name)
        for key, value in attributes.items():
            span.set_attribute(key, str(value))

        try:
            yield span
        except Exception as e:
            from opentelemetry.trace import Status, StatusCode
            span.set_status(Status(StatusCode.ERROR, str(e)))
            raise
        finally:
            span.end()
            from opentelemetry.sdk.trace import Span as SDKSpan
            if isinstance(span, SDKSpan):
                self.spans.append(span)

    def get_resource_spans(self) -> Dict[str, Any]:
        spans_json = []
        for span in self.spans:
            span_context = span.get_span_context()
            spans_json.append(
                {
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
                    'status': {'code': 'STATUS_CODE_OK'},
                }
            )

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

        self.spans.clear()
        return result


class XRayBackend(TraceBackend):
    def __init__(self):
        try:
            from aws_xray_sdk.core import xray_recorder
            xray_recorder.configure(service='aws-api-mcp-server', context_missing='LOG_ERROR')
            self.recorder = xray_recorder
        except ImportError:
            logger.warning('AWS X-Ray SDK not available, using no-op backend')
            self.recorder = None

    @contextmanager
    def create_span(self, name: str, attributes: Dict[str, Any]):
        if not self.recorder:
            yield None
            return
        if name == 'aws_cli_command':
            try:
                with self.recorder.in_segment(name) as segment:
                    segment.put_annotation('service', 'aws-api-mcp-server')
                    for key, value in attributes.items():
                        clean_key = key.replace('.', '_').replace('-', '_')
                        if len(str(value)) <= 50:
                            segment.put_annotation(clean_key, str(value))
                        else:
                            segment.put_metadata(clean_key, str(value))
                    yield segment
            except Exception as e:
                logger.debug(f'X-Ray segment creation failed: {e}')
                yield None
        else:      
            try:
                current_segment = self.recorder.current_segment()
                if current_segment:
                    with self.recorder.in_subsegment(name) as subsegment:
                        for key, value in attributes.items():
                            clean_key = key.replace('.', '_').replace('-', '_')
                            if len(str(value)) <= 50:
                                subsegment.put_annotation(clean_key, str(value))
                            else:
                                subsegment.put_metadata(clean_key, str(value))
                        yield subsegment
                else:
                   
                    yield None
            except Exception as e:
                logger.debug(f'X-Ray subsegment creation failed: {e}')
                yield None

    def get_resource_spans(self) -> Dict[str, Any]:
        return {'resource': {}, 'scopeSpans': []}


class TraceManager: 
    def __init__(self, backend: TraceBackend):
        self.backend = backend

    @contextmanager
    def trace(self, operation: str, **attributes):        
        try:
            with self.backend.create_span(operation, attributes) as span:
                yield span
        except Exception as e:
            logger.debug(f'Tracing failed for {operation}: {e}')
            yield None

    def instrument(self, operation_name: str):
        def decorator(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):          
                attributes = self._get_standard_attributes(operation_name, args, kwargs)
                with self.trace(operation_name, **attributes):
                    return await func(*args, **kwargs)

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                attributes = self._get_standard_attributes(operation_name, args, kwargs)
                with self.trace(operation_name, **attributes):
                    return func(*args, **kwargs)

            return (
                async_wrapper
                if hasattr(func, '__code__') and func.__code__.co_flags & 0x80
                else sync_wrapper
            )
        return decorator

    def _get_standard_attributes(self, operation_name: str, args, kwargs) -> Dict[str, Any]:
        attributes = {'mcp.server.name': 'aws-api-mcp-server', 'mcp.operation': operation_name} 
        if 'cli_command' in kwargs:
            cli_command = kwargs['cli_command']
            attributes['mcp.tool.name'] = 'call_aws'
            attributes['aws.cli.command'] = cli_command
            parts = cli_command.split()
            if len(parts) >= 2 and parts[0] == 'aws':
                attributes['aws.service'] = parts[1]
                if len(parts) >= 3:
                    attributes['aws.operation'] = parts[2]
        return attributes

    def get_resource_spans(self) -> Dict[str, Any]:
        return self.backend.get_resource_spans()


def get_trace_backend() -> TraceBackend:    
    backend_type = os.getenv('AWS_MCP_TRACING_BACKEND', 'disabled').lower()
    if backend_type == 'otlp':
        return OTLPBackend()
    elif backend_type == 'xray':
        return XRayBackend()
    else:
        return NoOpBackend()

trace_manager = TraceManager(get_trace_backend())
