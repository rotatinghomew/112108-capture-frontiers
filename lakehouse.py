"""Compatibility import surface for the local lakehouse pipeline."""

from imaging_lake.engine import LakeEngine, replay
from imaging_lake.store import LakeStore

Lakehouse = LakeStore
Pipeline = LakeEngine

__all__ = ["LakeEngine", "LakeStore", "Lakehouse", "Pipeline", "replay"]
