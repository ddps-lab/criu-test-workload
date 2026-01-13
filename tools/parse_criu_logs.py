#!/usr/bin/env python3
"""
CRIU Log Parser for Simulation

Parses CRIU logs (dump, restore, lazy-pages, object-storage, prefetch)
and exports structured data for simulation analysis.

Log Format (CRIU standard):
    (00.123456) <pid> <message>

Extended format for simulation (object-storage, prefetch):
    (00.123456) <pid> objstor: FETCH key=<key> offset=<offset> len=<len> dur_ms=<duration>
    (00.123456) <pid> prefetch: QUEUE iov_idx=<idx> priority=<priority>
    (00.123456) <pid> prefetch: HIT iov_idx=<idx>
    (00.123456) <pid> prefetch: MISS iov_idx=<idx>
"""

import argparse
import json
import re
import sys
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List, Dict, Any, Optional
from enum import Enum


class EventType(Enum):
    """Event types for simulation"""
    # Dump events
    DUMP_START = "dump_start"
    DUMP_PAGES = "dump_pages"
    DUMP_END = "dump_end"

    # Restore events
    RESTORE_START = "restore_start"
    RESTORE_PAGES = "restore_pages"
    RESTORE_END = "restore_end"

    # Lazy pages events
    LAZY_FAULT = "lazy_fault"
    LAZY_SERVE = "lazy_serve"

    # Object storage events (new structured format)
    OBJSTOR_FETCH_START = "objstor_fetch_start"
    OBJSTOR_FETCH_DONE = "objstor_fetch_done"
    OBJSTOR_FETCH_ERROR = "objstor_fetch_error"
    OBJSTOR_SESSION_CREATE = "objstor_session_create"
    OBJSTOR_SESSION_CREATED = "objstor_session_created"
    OBJSTOR_SESSION_ERROR = "objstor_session_error"
    # Legacy compatibility
    OBJSTOR_FETCH = "objstor_fetch"
    OBJSTOR_ERROR = "objstor_error"

    # Prefetch events (new structured format)
    PREFETCH_QUEUE = "prefetch_queue"
    PREFETCH_DEQUEUE = "prefetch_dequeue"
    PREFETCH_WORKER_START = "prefetch_worker_start"
    PREFETCH_WORKER_DONE = "prefetch_worker_done"
    PREFETCH_WORKER_ERROR = "prefetch_worker_error"
    PREFETCH_CACHE_HIT = "prefetch_cache_hit"
    PREFETCH_CACHE_MISS = "prefetch_cache_miss"
    PREFETCH_CACHE_STORE = "prefetch_cache_store"
    PREFETCH_CONTROLLER_FAULT = "prefetch_controller_fault"
    PREFETCH_CONTROLLER_PROMOTE = "prefetch_controller_promote"
    PREFETCH_CONTROLLER_REMOVE = "prefetch_controller_remove"
    PREFETCH_STATS = "prefetch_stats"
    # Legacy compatibility
    PREFETCH_HIT = "prefetch_hit"
    PREFETCH_MISS = "prefetch_miss"
    PREFETCH_COMPLETE = "prefetch_complete"

    # Generic
    INFO = "info"
    ERROR = "error"


@dataclass
class LogEvent:
    """Parsed log event"""
    timestamp: float
    pid: int
    event_type: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    raw_line: str = ""


@dataclass
class SimulationData:
    """Aggregated simulation data"""
    dump_events: List[LogEvent] = field(default_factory=list)
    restore_events: List[LogEvent] = field(default_factory=list)
    lazy_pages_events: List[LogEvent] = field(default_factory=list)
    objstor_events: List[LogEvent] = field(default_factory=list)
    prefetch_events: List[LogEvent] = field(default_factory=list)
    timeline: List[LogEvent] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)


class CRIULogParser:
    """Parse all types of CRIU logs for simulation"""

    # CRIU log line pattern: (00.123456) <pid> <message>
    LOG_PATTERN = re.compile(r'\(\s*(\d+\.\d+)\)\s+(\d+)\s+(.*)')

    # =======================================================================
    # Object storage patterns (new structured format from object-storage.h)
    # =======================================================================
    # objstor: FETCH_START key=<key> offset=<offset> len=<len>
    OBJSTOR_FETCH_START_PATTERN = re.compile(
        r'objstor:\s*FETCH_START\s+key=(\S+)\s+offset=(\d+)\s+len=(\d+)'
    )
    # objstor: FETCH_DONE key=<key> offset=<offset> len=<len> dur_ms=<duration>
    OBJSTOR_FETCH_DONE_PATTERN = re.compile(
        r'objstor:\s*FETCH_DONE\s+key=(\S+)\s+offset=(\d+)\s+len=(\d+)\s+dur_ms=(\d+\.?\d*)'
    )
    # objstor: FETCH_ERROR key=<key> offset=<offset> len=<len> error=<code>
    OBJSTOR_FETCH_ERROR_PATTERN = re.compile(
        r'objstor:\s*FETCH_ERROR\s+key=(\S+)\s+offset=(\d+)\s+len=(\d+)\s+error=(-?\d+)'
    )
    # objstor: SESSION_CREATE
    OBJSTOR_SESSION_CREATE_PATTERN = re.compile(r'objstor:\s*SESSION_CREATE\b')
    # objstor: SESSION_CREATED expires=<expiration>
    OBJSTOR_SESSION_CREATED_PATTERN = re.compile(
        r'objstor:\s*SESSION_CREATED\s+expires=(\d+)'
    )
    # objstor: SESSION_ERROR http_code=<code>
    OBJSTOR_SESSION_ERROR_PATTERN = re.compile(
        r'objstor:\s*SESSION_ERROR\s+http_code=(-?\d+)'
    )
    # Legacy: objstor: FETCH key=<key> offset=<offset> len=<len> dur_ms=<duration>
    OBJSTOR_FETCH_PATTERN = re.compile(
        r'objstor:\s*FETCH\s+key=(\S+)\s+offset=(\d+)\s+len=(\d+)(?:\s+dur_ms=(\d+\.?\d*))?'
    )

    # =======================================================================
    # Prefetch patterns (new structured format from object-storage.h)
    # =======================================================================
    # prefetch: QUEUE iov_idx=<idx> iov_start=0x<start> iov_end=0x<end> priority=<priority>
    PREFETCH_QUEUE_PATTERN = re.compile(
        r'prefetch:\s*QUEUE\s+iov_idx=(\d+)\s+iov_start=0x([0-9a-fA-F]+)\s+iov_end=0x([0-9a-fA-F]+)\s+priority=(\d+)'
    )
    # prefetch: DEQUEUE iov_idx=<idx> worker=<worker_id>
    PREFETCH_DEQUEUE_PATTERN = re.compile(
        r'prefetch:\s*DEQUEUE\s+iov_idx=(\d+)\s+worker=(\d+)'
    )
    # prefetch: WORKER_START worker=<id> iov_idx=<idx>
    PREFETCH_WORKER_START_PATTERN = re.compile(
        r'prefetch:\s*WORKER_START\s+worker=(\d+)\s+iov_idx=(\d+)'
    )
    # prefetch: WORKER_DONE worker=<id> iov_idx=<idx> dur_ms=<duration>
    PREFETCH_WORKER_DONE_PATTERN = re.compile(
        r'prefetch:\s*WORKER_DONE\s+worker=(\d+)\s+iov_idx=(\d+)\s+dur_ms=(\d+\.?\d*)'
    )
    # prefetch: WORKER_ERROR worker=<id> iov_idx=<idx> error=<code>
    PREFETCH_WORKER_ERROR_PATTERN = re.compile(
        r'prefetch:\s*WORKER_ERROR\s+worker=(\d+)\s+iov_idx=(\d+)\s+error=(-?\d+)'
    )
    # prefetch: CACHE_HIT iov_idx=<idx>
    PREFETCH_CACHE_HIT_PATTERN = re.compile(
        r'prefetch:\s*CACHE_HIT\s+iov_idx=(\d+)'
    )
    # prefetch: CACHE_MISS iov_idx=<idx>
    PREFETCH_CACHE_MISS_PATTERN = re.compile(
        r'prefetch:\s*CACHE_MISS\s+iov_idx=(\d+)'
    )
    # prefetch: CACHE_STORE iov_idx=<idx> size=<size>
    PREFETCH_CACHE_STORE_PATTERN = re.compile(
        r'prefetch:\s*CACHE_STORE\s+iov_idx=(\d+)\s+size=(\d+)'
    )
    # prefetch: CONTROLLER_FAULT iov_idx=<idx> pattern=<type> confidence=<conf>
    PREFETCH_CONTROLLER_FAULT_PATTERN = re.compile(
        r'prefetch:\s*CONTROLLER_FAULT\s+iov_idx=(\d+)\s+pattern=(\d+)\s+confidence=(\d+\.?\d*)'
    )
    # prefetch: CONTROLLER_PROMOTE iov_idx=<idx> old_prio=<old> new_prio=<new>
    PREFETCH_CONTROLLER_PROMOTE_PATTERN = re.compile(
        r'prefetch:\s*CONTROLLER_PROMOTE\s+iov_idx=(\d+)\s+old_prio=(\d+)\s+new_prio=(\d+)'
    )
    # prefetch: CONTROLLER_REMOVE iov_idx=<idx> reason=<reason>
    PREFETCH_CONTROLLER_REMOVE_PATTERN = re.compile(
        r'prefetch:\s*CONTROLLER_REMOVE\s+iov_idx=(\d+)\s+reason=(\S+)'
    )
    # prefetch: STATS requests=<n> completed=<n> failed=<n> hits=<n> misses=<n>
    PREFETCH_STATS_PATTERN = re.compile(
        r'prefetch:\s*STATS\s+requests=(\d+)\s+completed=(\d+)\s+failed=(\d+)\s+hits=(\d+)\s+misses=(\d+)'
    )
    # Legacy patterns for compatibility
    PREFETCH_QUEUE_LEGACY_PATTERN = re.compile(
        r'prefetch:\s*(?:PREFETCH:)?\s*(?:Queued|QUEUE)\s+IOV\s*\[?(?:0x)?([0-9a-fA-F]+)[-\s](?:0x)?([0-9a-fA-F]+)\]?\s*(?:with\s+)?priority[=\s]*(\d+)'
    )
    PREFETCH_WORKER_LEGACY_PATTERN = re.compile(
        r'prefetch:\s*(?:PREFETCH:)?\s*Worker\s+(\d+)\s+(processing|Successfully|Failed)'
    )

    # Page fault pattern
    PAGE_FAULT_PATTERN = re.compile(
        r'(?:uffd|page|fault).*(?:0x)?([0-9a-fA-F]+)'
    )

    # Dump/restore patterns
    DUMP_PAGES_PATTERN = re.compile(
        r'(?:Dumping|Writing)\s+(?:pages|memory).*?(\d+)\s*(?:pages|KB|MB)?'
    )
    RESTORE_PAGES_PATTERN = re.compile(
        r'(?:Restoring|Reading)\s+(?:pages|memory).*?(\d+)\s*(?:pages|KB|MB)?'
    )

    def __init__(self):
        self.events: List[LogEvent] = []

    def parse_line(self, line: str) -> Optional[LogEvent]:
        """Parse a single CRIU log line"""
        line = line.strip()
        if not line:
            return None

        match = self.LOG_PATTERN.match(line)
        if not match:
            return None

        timestamp = float(match.group(1))
        pid = int(match.group(2))
        message = match.group(3)

        event = LogEvent(
            timestamp=timestamp,
            pid=pid,
            event_type=EventType.INFO.value,
            message=message,
            raw_line=line
        )

        # Classify event type
        self._classify_event(event)

        return event

    def _classify_event(self, event: LogEvent):
        """Classify event type based on message content"""
        msg = event.message.lower()

        # Object storage events
        if 'objstor:' in event.message:
            self._parse_objstor_event(event)
            return

        # Prefetch events
        if 'prefetch:' in event.message:
            self._parse_prefetch_event(event)
            return

        # Page fault events
        if 'uffd' in msg or ('page' in msg and 'fault' in msg):
            event.event_type = EventType.LAZY_FAULT.value
            match = self.PAGE_FAULT_PATTERN.search(event.message)
            if match:
                event.details['address'] = match.group(1)
            return

        # Dump events
        if 'dump' in msg:
            if 'start' in msg or 'begin' in msg:
                event.event_type = EventType.DUMP_START.value
            elif 'finish' in msg or 'end' in msg or 'complete' in msg:
                event.event_type = EventType.DUMP_END.value
            else:
                match = self.DUMP_PAGES_PATTERN.search(event.message)
                if match:
                    event.event_type = EventType.DUMP_PAGES.value
                    event.details['pages'] = int(match.group(1))
            return

        # Restore events
        if 'restor' in msg:
            if 'start' in msg or 'begin' in msg:
                event.event_type = EventType.RESTORE_START.value
            elif 'finish' in msg or 'end' in msg or 'complete' in msg:
                event.event_type = EventType.RESTORE_END.value
            else:
                match = self.RESTORE_PAGES_PATTERN.search(event.message)
                if match:
                    event.event_type = EventType.RESTORE_PAGES.value
                    event.details['pages'] = int(match.group(1))
            return

        # Error events
        if 'error' in msg or 'fail' in msg:
            event.event_type = EventType.ERROR.value

    def _parse_objstor_event(self, event: LogEvent):
        """Parse object storage specific events (new structured format)"""
        # FETCH_START event (new structured format)
        match = self.OBJSTOR_FETCH_START_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.OBJSTOR_FETCH_START.value
            event.details['key'] = match.group(1)
            event.details['offset'] = int(match.group(2))
            event.details['length'] = int(match.group(3))
            return

        # FETCH_DONE event (new structured format)
        match = self.OBJSTOR_FETCH_DONE_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.OBJSTOR_FETCH_DONE.value
            event.details['key'] = match.group(1)
            event.details['offset'] = int(match.group(2))
            event.details['length'] = int(match.group(3))
            event.details['duration_ms'] = float(match.group(4))
            return

        # FETCH_ERROR event (new structured format)
        match = self.OBJSTOR_FETCH_ERROR_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.OBJSTOR_FETCH_ERROR.value
            event.details['key'] = match.group(1)
            event.details['offset'] = int(match.group(2))
            event.details['length'] = int(match.group(3))
            event.details['error_code'] = int(match.group(4))
            return

        # SESSION_CREATE event
        match = self.OBJSTOR_SESSION_CREATE_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.OBJSTOR_SESSION_CREATE.value
            return

        # SESSION_CREATED event
        match = self.OBJSTOR_SESSION_CREATED_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.OBJSTOR_SESSION_CREATED.value
            event.details['expiration'] = int(match.group(1))
            return

        # SESSION_ERROR event
        match = self.OBJSTOR_SESSION_ERROR_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.OBJSTOR_SESSION_ERROR.value
            event.details['http_code'] = int(match.group(1))
            return

        # Legacy FETCH event (for backward compatibility)
        match = self.OBJSTOR_FETCH_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.OBJSTOR_FETCH.value
            event.details['key'] = match.group(1)
            event.details['offset'] = int(match.group(2))
            event.details['length'] = int(match.group(3))
            if match.group(4):
                event.details['duration_ms'] = float(match.group(4))
            return

        # Generic object storage error
        if 'error' in event.message.lower() or 'fail' in event.message.lower():
            event.event_type = EventType.OBJSTOR_ERROR.value

    def _parse_prefetch_event(self, event: LogEvent):
        """Parse prefetch specific events (new structured format)"""
        # QUEUE event (new structured format)
        match = self.PREFETCH_QUEUE_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.PREFETCH_QUEUE.value
            event.details['iov_idx'] = int(match.group(1))
            event.details['iov_start'] = match.group(2)
            event.details['iov_end'] = match.group(3)
            event.details['priority'] = int(match.group(4))
            return

        # DEQUEUE event
        match = self.PREFETCH_DEQUEUE_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.PREFETCH_DEQUEUE.value
            event.details['iov_idx'] = int(match.group(1))
            event.details['worker_id'] = int(match.group(2))
            return

        # WORKER_START event
        match = self.PREFETCH_WORKER_START_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.PREFETCH_WORKER_START.value
            event.details['worker_id'] = int(match.group(1))
            event.details['iov_idx'] = int(match.group(2))
            return

        # WORKER_DONE event
        match = self.PREFETCH_WORKER_DONE_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.PREFETCH_WORKER_DONE.value
            event.details['worker_id'] = int(match.group(1))
            event.details['iov_idx'] = int(match.group(2))
            event.details['duration_ms'] = float(match.group(3))
            return

        # WORKER_ERROR event
        match = self.PREFETCH_WORKER_ERROR_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.PREFETCH_WORKER_ERROR.value
            event.details['worker_id'] = int(match.group(1))
            event.details['iov_idx'] = int(match.group(2))
            event.details['error_code'] = int(match.group(3))
            return

        # CACHE_HIT event
        match = self.PREFETCH_CACHE_HIT_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.PREFETCH_CACHE_HIT.value
            event.details['iov_idx'] = int(match.group(1))
            return

        # CACHE_MISS event
        match = self.PREFETCH_CACHE_MISS_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.PREFETCH_CACHE_MISS.value
            event.details['iov_idx'] = int(match.group(1))
            return

        # CACHE_STORE event
        match = self.PREFETCH_CACHE_STORE_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.PREFETCH_CACHE_STORE.value
            event.details['iov_idx'] = int(match.group(1))
            event.details['size'] = int(match.group(2))
            return

        # CONTROLLER_FAULT event
        match = self.PREFETCH_CONTROLLER_FAULT_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.PREFETCH_CONTROLLER_FAULT.value
            event.details['iov_idx'] = int(match.group(1))
            event.details['pattern_type'] = int(match.group(2))
            event.details['confidence'] = float(match.group(3))
            return

        # CONTROLLER_PROMOTE event
        match = self.PREFETCH_CONTROLLER_PROMOTE_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.PREFETCH_CONTROLLER_PROMOTE.value
            event.details['iov_idx'] = int(match.group(1))
            event.details['old_priority'] = int(match.group(2))
            event.details['new_priority'] = int(match.group(3))
            return

        # CONTROLLER_REMOVE event
        match = self.PREFETCH_CONTROLLER_REMOVE_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.PREFETCH_CONTROLLER_REMOVE.value
            event.details['iov_idx'] = int(match.group(1))
            event.details['reason'] = match.group(2)
            return

        # STATS event
        match = self.PREFETCH_STATS_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.PREFETCH_STATS.value
            event.details['total_requests'] = int(match.group(1))
            event.details['completed'] = int(match.group(2))
            event.details['failed'] = int(match.group(3))
            event.details['cache_hits'] = int(match.group(4))
            event.details['cache_misses'] = int(match.group(5))
            return

        # Legacy QUEUE pattern
        match = self.PREFETCH_QUEUE_LEGACY_PATTERN.search(event.message)
        if match:
            event.event_type = EventType.PREFETCH_QUEUE.value
            event.details['iov_start'] = match.group(1)
            event.details['iov_end'] = match.group(2)
            event.details['priority'] = int(match.group(3))
            return

        # Legacy Worker events
        match = self.PREFETCH_WORKER_LEGACY_PATTERN.search(event.message)
        if match:
            worker_id = int(match.group(1))
            action = match.group(2)
            event.details['worker_id'] = worker_id
            if 'success' in action.lower():
                event.event_type = EventType.PREFETCH_COMPLETE.value
                event.details['success'] = True
            elif 'fail' in action.lower():
                event.event_type = EventType.PREFETCH_COMPLETE.value
                event.details['success'] = False
            return

        # Legacy cache hit/miss (simple string match)
        if 'hit' in event.message.lower():
            event.event_type = EventType.PREFETCH_HIT.value
        elif 'miss' in event.message.lower():
            event.event_type = EventType.PREFETCH_MISS.value

    def parse_file(self, filepath: str) -> List[LogEvent]:
        """Parse a log file"""
        events = []
        try:
            with open(filepath, 'r') as f:
                for line in f:
                    event = self.parse_line(line)
                    if event:
                        events.append(event)
        except FileNotFoundError:
            print(f"Warning: Log file not found: {filepath}", file=sys.stderr)
        except Exception as e:
            print(f"Error parsing {filepath}: {e}", file=sys.stderr)

        return events

    def parse_dump_log(self, filepath: str) -> List[LogEvent]:
        """Parse dump log - extract dump timing, pages written"""
        events = self.parse_file(filepath)
        dump_events = [e for e in events if e.event_type.startswith('dump')]
        return dump_events

    def parse_restore_log(self, filepath: str) -> List[LogEvent]:
        """Parse restore log - extract restore timing, pages restored"""
        events = self.parse_file(filepath)
        restore_events = [e for e in events if e.event_type.startswith('restore')]
        return restore_events

    def parse_lazy_pages_log(self, filepath: str) -> List[LogEvent]:
        """Parse lazy-pages log - extract page faults, fetches"""
        events = self.parse_file(filepath)
        lazy_events = [e for e in events if e.event_type.startswith('lazy') or
                       e.event_type.startswith('objstor') or
                       e.event_type.startswith('prefetch')]
        return lazy_events

    def parse_all_logs(self, dump_log: str = None, restore_log: str = None,
                       lazy_log: str = None) -> SimulationData:
        """Parse all logs and aggregate for simulation"""
        sim_data = SimulationData()
        all_events = []

        if dump_log:
            events = self.parse_file(dump_log)
            sim_data.dump_events = [e for e in events if e.event_type.startswith('dump')]
            all_events.extend(events)

        if restore_log:
            events = self.parse_file(restore_log)
            sim_data.restore_events = [e for e in events if e.event_type.startswith('restore')]
            all_events.extend(events)

        if lazy_log:
            events = self.parse_file(lazy_log)
            sim_data.lazy_pages_events = [e for e in events if e.event_type.startswith('lazy')]
            sim_data.objstor_events = [e for e in events if e.event_type.startswith('objstor')]
            sim_data.prefetch_events = [e for e in events if e.event_type.startswith('prefetch')]
            all_events.extend(events)

        # Sort by timestamp
        all_events.sort(key=lambda e: e.timestamp)
        sim_data.timeline = all_events

        # Calculate summary
        sim_data.summary = self._calculate_summary(sim_data)

        return sim_data

    def _calculate_summary(self, sim_data: SimulationData) -> Dict[str, Any]:
        """Calculate summary statistics (supports both new and legacy formats)"""

        # Count cache hits/misses from both new and legacy event types
        cache_hits = len([e for e in sim_data.prefetch_events
                         if e.event_type in (EventType.PREFETCH_CACHE_HIT.value,
                                             EventType.PREFETCH_HIT.value)])
        cache_misses = len([e for e in sim_data.prefetch_events
                          if e.event_type in (EventType.PREFETCH_CACHE_MISS.value,
                                              EventType.PREFETCH_MISS.value)])

        # Count fetch events from both new and legacy formats
        fetch_start_count = len([e for e in sim_data.objstor_events
                                if e.event_type == EventType.OBJSTOR_FETCH_START.value])
        fetch_done_count = len([e for e in sim_data.objstor_events
                               if e.event_type == EventType.OBJSTOR_FETCH_DONE.value])
        fetch_error_count = len([e for e in sim_data.objstor_events
                                if e.event_type == EventType.OBJSTOR_FETCH_ERROR.value])
        fetch_legacy_count = len([e for e in sim_data.objstor_events
                                 if e.event_type == EventType.OBJSTOR_FETCH.value])

        summary = {
            'dump_event_count': len(sim_data.dump_events),
            'restore_event_count': len(sim_data.restore_events),
            'lazy_fault_count': len([e for e in sim_data.lazy_pages_events
                                      if e.event_type == EventType.LAZY_FAULT.value]),
            # Object storage fetch statistics
            'objstor_fetch_start_count': fetch_start_count,
            'objstor_fetch_done_count': fetch_done_count,
            'objstor_fetch_error_count': fetch_error_count,
            'objstor_fetch_count': fetch_start_count + fetch_legacy_count,  # Combined count
            # Session statistics
            'objstor_session_count': len([e for e in sim_data.objstor_events
                                         if e.event_type == EventType.OBJSTOR_SESSION_CREATED.value]),
            # Prefetch queue statistics
            'prefetch_queue_count': len([e for e in sim_data.prefetch_events
                                          if e.event_type == EventType.PREFETCH_QUEUE.value]),
            'prefetch_dequeue_count': len([e for e in sim_data.prefetch_events
                                           if e.event_type == EventType.PREFETCH_DEQUEUE.value]),
            # Prefetch worker statistics
            'prefetch_worker_start_count': len([e for e in sim_data.prefetch_events
                                               if e.event_type == EventType.PREFETCH_WORKER_START.value]),
            'prefetch_worker_done_count': len([e for e in sim_data.prefetch_events
                                              if e.event_type == EventType.PREFETCH_WORKER_DONE.value]),
            'prefetch_worker_error_count': len([e for e in sim_data.prefetch_events
                                               if e.event_type == EventType.PREFETCH_WORKER_ERROR.value]),
            # Cache statistics (combined from new and legacy formats)
            'prefetch_cache_hit_count': cache_hits,
            'prefetch_cache_miss_count': cache_misses,
            'prefetch_cache_store_count': len([e for e in sim_data.prefetch_events
                                              if e.event_type == EventType.PREFETCH_CACHE_STORE.value]),
            # Controller statistics
            'prefetch_controller_fault_count': len([e for e in sim_data.prefetch_events
                                                   if e.event_type == EventType.PREFETCH_CONTROLLER_FAULT.value]),
            'prefetch_controller_promote_count': len([e for e in sim_data.prefetch_events
                                                     if e.event_type == EventType.PREFETCH_CONTROLLER_PROMOTE.value]),
            # Legacy compatibility fields
            'prefetch_hit_count': cache_hits,
            'prefetch_miss_count': cache_misses,
            'total_events': len(sim_data.timeline),
        }

        # Calculate duration if we have start/end events
        if sim_data.timeline:
            summary['start_timestamp'] = sim_data.timeline[0].timestamp
            summary['end_timestamp'] = sim_data.timeline[-1].timestamp
            summary['total_duration_sec'] = summary['end_timestamp'] - summary['start_timestamp']

        # Calculate objstor fetch statistics from FETCH_DONE events (new format)
        fetch_durations = [e.details.get('duration_ms', 0)
                          for e in sim_data.objstor_events
                          if e.event_type == EventType.OBJSTOR_FETCH_DONE.value and 'duration_ms' in e.details]
        # Also include legacy FETCH events
        fetch_durations.extend([e.details.get('duration_ms', 0)
                               for e in sim_data.objstor_events
                               if e.event_type == EventType.OBJSTOR_FETCH.value and 'duration_ms' in e.details])

        if fetch_durations:
            summary['objstor_avg_fetch_ms'] = sum(fetch_durations) / len(fetch_durations)
            summary['objstor_total_fetch_ms'] = sum(fetch_durations)
            summary['objstor_min_fetch_ms'] = min(fetch_durations)
            summary['objstor_max_fetch_ms'] = max(fetch_durations)

        # Calculate total bytes fetched
        total_bytes = sum(e.details.get('length', 0)
                         for e in sim_data.objstor_events
                         if e.event_type in (EventType.OBJSTOR_FETCH_DONE.value,
                                             EventType.OBJSTOR_FETCH.value))
        if total_bytes > 0:
            summary['objstor_total_bytes_fetched'] = total_bytes

        # Calculate prefetch worker durations
        worker_durations = [e.details.get('duration_ms', 0)
                          for e in sim_data.prefetch_events
                          if e.event_type == EventType.PREFETCH_WORKER_DONE.value and 'duration_ms' in e.details]
        if worker_durations:
            summary['prefetch_avg_worker_ms'] = sum(worker_durations) / len(worker_durations)
            summary['prefetch_total_worker_ms'] = sum(worker_durations)

        # Calculate prefetch hit rate
        total_lookups = cache_hits + cache_misses
        if total_lookups > 0:
            summary['prefetch_hit_rate'] = cache_hits / total_lookups

        # Get final STATS event if present
        stats_events = [e for e in sim_data.prefetch_events
                       if e.event_type == EventType.PREFETCH_STATS.value]
        if stats_events:
            final_stats = stats_events[-1].details
            summary['final_prefetch_stats'] = final_stats

        return summary

    def export_for_simulator(self, sim_data: SimulationData, output_file: str):
        """Export parsed data in simulator-compatible format"""
        # Convert dataclasses to dicts
        output = {
            'dump_events': [asdict(e) for e in sim_data.dump_events],
            'restore_events': [asdict(e) for e in sim_data.restore_events],
            'lazy_pages_events': [asdict(e) for e in sim_data.lazy_pages_events],
            'objstor_events': [asdict(e) for e in sim_data.objstor_events],
            'prefetch_events': [asdict(e) for e in sim_data.prefetch_events],
            'timeline': [asdict(e) for e in sim_data.timeline],
            'summary': sim_data.summary
        }

        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2)

        print(f"Exported simulation data to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Parse CRIU logs for simulation analysis'
    )
    parser.add_argument('--dump-log', '-d', help='Path to dump log file')
    parser.add_argument('--restore-log', '-r', help='Path to restore log file')
    parser.add_argument('--lazy-log', '-l', help='Path to lazy-pages log file')
    parser.add_argument('--output', '-o', default='simulation_input.json',
                       help='Output JSON file (default: simulation_input.json)')
    parser.add_argument('--summary', '-s', action='store_true',
                       help='Print summary to stdout')

    args = parser.parse_args()

    if not any([args.dump_log, args.restore_log, args.lazy_log]):
        parser.error('At least one log file must be specified')

    # Parse logs
    log_parser = CRIULogParser()
    sim_data = log_parser.parse_all_logs(
        dump_log=args.dump_log,
        restore_log=args.restore_log,
        lazy_log=args.lazy_log
    )

    # Export to JSON
    log_parser.export_for_simulator(sim_data, args.output)

    # Print summary if requested
    if args.summary:
        print("\n=== CRIU Log Summary ===")
        for key, value in sim_data.summary.items():
            if isinstance(value, float):
                print(f"  {key}: {value:.3f}")
            else:
                print(f"  {key}: {value}")


if __name__ == '__main__':
    main()
