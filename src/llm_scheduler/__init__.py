"""
Task Scheduling System

This module defines the core concepts and components of a task scheduling system.

Core Concepts:

Task:
    A Task represents a unit of work that can be scheduled for execution.
    It can be either a one-time task or a recurring task (e.g., periodic, cron-based).
    A Task defines the work to be done but does not represent an actual execution.

Job:
    A Job represents a single execution of a Task.
    When a Task is scheduled, one or more Job instances are created based on the Task's configuration.
    Each Job instance corresponds to an actual execution of the Task.

Relationships:
    - A Task can have multiple Job instances, each representing a single execution.
"""

from .backends import *

__all__ = ["backends"]
