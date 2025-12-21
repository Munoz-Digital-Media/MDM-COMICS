"""
Training data loader scaffolding for CGC grading model.

Produces batches with:
- image tensor placeholder (currently numpy arrays; swap for torch tensors when model is ready)
- label feature vectors from LabelRegistry
- optional metadata (label slug, path)

Note: This intentionally avoids pulling the DB. Feed it a manifest of
{image_path, label_slug} prepared by your ingestion pipeline or a CSV export.
"""
import io
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple, Dict

import numpy as np
from PIL import Image

from app.ml.grade_estimator import GradeEstimatorService


@dataclass
class Sample:
    image_path: Path
    label_slug: Optional[str] = None


class GradingDataset:
    """
    Lightweight dataset that pairs local images with label metadata.
    Swap the image preprocessing to torch transforms when wiring the model.
    """

    def __init__(
        self,
        samples: Iterable[Sample],
        include_reference_texts: bool = True,
        embed_reference_texts: bool = True,
        embedding_dim: int = 128,
    ):
        self.samples: List[Sample] = list(samples)
        self.estimator = GradeEstimatorService()
        self.include_reference_texts = include_reference_texts
        self.embed_reference_texts = embed_reference_texts
        self.embedding_dim = embedding_dim

    def __len__(self) -> int:
        return len(self.samples)

    def __getitem__(self, idx: int) -> Dict[str, any]:
        sample = self.samples[idx]
        image_bytes = Path(sample.image_path).read_bytes()
        features = self.estimator.build_features(
            image_bytes=image_bytes,
            label_slug=sample.label_slug,
            include_reference_texts=self.include_reference_texts,
            embed_reference_texts=self.embed_reference_texts,
            embedding_dim=self.embedding_dim,
        )
        return {
            "image": features["image"],  # numpy array placeholder
            "label_features": features["label_features"],
            "reference_texts": features.get("reference_texts"),
            "reference_embedding": features.get("reference_embedding"),
            "label_slug": sample.label_slug,
            "path": str(sample.image_path),
        }


def collate_fn(batch: List[Dict]) -> Dict[str, any]:
    """
    Simple collate that stacks image arrays and merges label feature dicts.
    Replace with torch.utils.data.dataloader default when using torch tensors.
    """
    images = [item["image"] for item in batch]
    label_vectors = [item["label_features"] for item in batch]
    label_slugs = [item["label_slug"] for item in batch]
    paths = [item["path"] for item in batch]

    # Stack images on a new axis; with torch, use torch.stack instead.
    image_batch = np.stack(images, axis=0)

    return {
        "images": image_batch,
        "label_features": label_vectors,
        "label_slugs": label_slugs,
        "paths": paths,
    }


__all__ = ["Sample", "GradingDataset", "collate_fn"]
