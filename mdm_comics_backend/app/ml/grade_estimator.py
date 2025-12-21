"""
AI Grade Estimation Service

This module handles the ML model for estimating CGC grades from comic book images.
Currently implements a placeholder that returns mock data.
Replace with actual model inference when training is complete.
"""
import io
import random
from pathlib import Path
from typing import Dict, Any, Optional

import numpy as np
from PIL import Image

from app.ml.label_registry import LabelRegistry
from app.ml.pdf_corpus import PdfCorpus
from app.ml.convention_events import ConventionEvents
from app.ml.text_embeddings import TextEmbedder, hash_embedding, merge_embeddings

# Uncomment when model is ready:
# import torch
# import torchvision.transforms as transforms
# from app.core.config import settings


class GradeEstimatorService:
    """
    Service for estimating comic book grades using computer vision.
    
    The model analyzes:
    - Corner wear and damage
    - Spine stress and rolling
    - Page quality and coloring
    - Centering (front and back)
    - Surface defects (tears, creases, stains)
    """
    
    def __init__(self):
        self.model = None
        self.model_version = "0.1.0-mock"
        self.device = "cpu"  # or "cuda" if GPU available
        self.label_registry = LabelRegistry()
        self.pdf_corpus = PdfCorpus()
        self.conventions = ConventionEvents()
        # Optional high-quality text embedder; uses hashed BoW fallback if no local model is present.
        local_model_dir = (Path(__file__).resolve().parents[3] / "assets" / "ml" / "text_encoder")
        self.text_embedder = TextEmbedder(model_path=local_model_dir)
        
        # TODO: Load actual model
        # self._load_model()
    
    def _load_model(self):
        """Load the trained PyTorch model"""
        # try:
        #     self.model = torch.load(settings.ML_MODEL_PATH, map_location=self.device)
        #     self.model.eval()
        #     self.model_version = "1.0.0"
        # except Exception as e:
        #     print(f"Warning: Could not load ML model: {e}")
        #     self.model = None
        pass
    
    def _preprocess_image(self, image: Image.Image) -> np.ndarray:
        """
        Preprocess image for model input.
        
        - Resize to model input size
        - Normalize pixel values
        - Convert to tensor format
        """
        # Standard preprocessing for vision models
        # transform = transforms.Compose([
        #     transforms.Resize((224, 224)),
        #     transforms.ToTensor(),
        #     transforms.Normalize(
        #         mean=[0.485, 0.456, 0.406],
        #         std=[0.229, 0.224, 0.225]
        #     )
        # ])
        # return transform(image)
        
        # Placeholder: just resize
        image = image.resize((224, 224))
        return np.array(image)

    def _label_features(self, label_slug: Optional[str]) -> Dict[str, float]:
        """Fetch label feature vector (0/1 flags and metadata) for a given label slug."""
        if not label_slug:
            return {}
        features = self.label_registry.feature_vector(label_slug)
        return features or {}

    def build_features(
        self,
        image_bytes: bytes,
        label_slug: Optional[str] = None,
        signer_name: Optional[str] = None,
        include_reference_texts: bool = False,
        embed_reference_texts: bool = True,
        embedding_dim: int = 128,
    ) -> Dict[str, Any]:
        """
        Build combined feature payload for training/inference.

        Returns a dict with:
        - image: preprocessed array (placeholder until torch pipeline is wired)
        - label_features: numeric vector derived from CGC label metadata
        - reference_texts: optional CGC grading PDFs (text) when include_reference_texts is True
        - reference_embedding: hashed bag-of-words embedding over CGC PDFs (optional)
        - convention_features: signer-level features from convention listings (if signer_name provided)
        """
        image = Image.open(io.BytesIO(image_bytes))
        image_features = self._preprocess_image(image)
        label_features = self._label_features(label_slug)
        convention_features = self.conventions.signer_features(signer_name)
        reference_texts = None
        reference_embedding = None
        if include_reference_texts:
            reference_texts = {
                slug: self.pdf_corpus.text(slug)
                for slug in self.pdf_corpus.slugs()
                if self.pdf_corpus.text(slug)
            }
            if embed_reference_texts and reference_texts:
                if self.text_embedder.model:
                    reference_embedding = self.text_embedder.embed_and_merge(reference_texts.values())
                else:
                    embeddings = [
                        hash_embedding(text, dim=embedding_dim)
                        for text in reference_texts.values()
                    ]
                    reference_embedding = merge_embeddings(embeddings)
        return {
            "image": image_features,
            "label_features": label_features,
            "convention_features": convention_features,
            "reference_texts": reference_texts,
            "reference_embedding": reference_embedding,
        }
    
    def _grade_to_label(self, grade: float) -> str:
        """Convert numeric grade to CGC label"""
        labels = {
            10.0: "Gem Mint",
            9.9: "Mint",
            9.8: "Near Mint/Mint",
            9.6: "Near Mint+",
            9.4: "Near Mint",
            9.2: "Near Mint-",
            9.0: "Very Fine/Near Mint",
            8.5: "Very Fine+",
            8.0: "Very Fine",
            7.5: "Very Fine-",
            7.0: "Fine/Very Fine",
            6.5: "Fine+",
            6.0: "Fine",
            5.5: "Fine-",
            5.0: "Very Good/Fine",
            4.5: "Very Good+",
            4.0: "Very Good",
            3.5: "Very Good-",
            3.0: "Good/Very Good",
            2.5: "Good+",
            2.0: "Good",
        }
        
        # Find closest grade
        closest = min(labels.keys(), key=lambda x: abs(x - grade))
        return f"{labels[closest]} ({grade})"
    
    async def estimate_grade(self, image_url: str) -> Dict[str, Any]:
        """
        Estimate grade from image URL.
        
        Returns:
            {
                "grade": 9.4,
                "confidence": 0.87,
                "grade_label": "Near Mint (9.4)",
                "factors": {...},
                "analysis": {...}
            }
        """
        # TODO: Download and process actual image
        # async with httpx.AsyncClient() as client:
        #     response = await client.get(image_url)
        #     image = Image.open(io.BytesIO(response.content))
        
        # For now, return mock data
        return self._mock_estimate()
    
    async def estimate_grade_from_bytes(self, image_bytes: bytes) -> Dict[str, Any]:
        """
        Estimate grade from image bytes (uploaded file).
        """
        try:
            image = Image.open(io.BytesIO(image_bytes))
            
            if self.model is not None:
                # Real inference
                # processed = self._preprocess_image(image)
                # with torch.no_grad():
                #     prediction = self.model(processed.unsqueeze(0))
                # return self._process_prediction(prediction)
                pass
            
            # Mock response
            return self._mock_estimate()
            
        except Exception as e:
            raise ValueError(f"Could not process image: {str(e)}")
    
    def _mock_estimate(self) -> Dict[str, Any]:
        """
        Generate mock grade estimate for development/testing.
        Replace with actual model inference.
        """
        # Simulate realistic grade distribution (most books are 7-9 range)
        grade = round(random.triangular(6.0, 9.8, 8.5), 1)
        
        # Ensure grade is on CGC scale (0.5 increments for lower grades, 0.2 for higher)
        if grade >= 9.0:
            grade = round(grade * 5) / 5  # 0.2 increments
        else:
            grade = round(grade * 2) / 2  # 0.5 increments
        
        confidence = round(random.uniform(0.75, 0.95), 2)
        
        # Generate factor scores
        factors = {
            "corners": round(random.uniform(grade - 0.5, min(grade + 0.5, 10)), 1),
            "spine": round(random.uniform(grade - 0.5, min(grade + 0.5, 10)), 1),
            "pages": round(random.uniform(grade - 0.3, min(grade + 0.3, 10)), 1),
            "centering": round(random.uniform(grade - 1.0, min(grade + 0.5, 10)), 1),
        }
        
        # Detailed analysis
        analysis = {
            "corners": {
                "score": factors["corners"],
                "notes": self._generate_corner_notes(factors["corners"])
            },
            "spine": {
                "score": factors["spine"],
                "notes": self._generate_spine_notes(factors["spine"])
            },
            "pages": {
                "score": factors["pages"],
                "notes": self._generate_page_notes(factors["pages"])
            },
            "centering": {
                "score": factors["centering"],
                "notes": self._generate_centering_notes(factors["centering"])
            },
            "defects": self._generate_defects(grade)
        }
        
        return {
            "grade": grade,
            "confidence": confidence,
            "grade_label": self._grade_to_label(grade),
            "factors": factors,
            "analysis": analysis
        }
    
    def _generate_corner_notes(self, score: float) -> str:
        if score >= 9.5:
            return "Corners are sharp and well-defined"
        elif score >= 9.0:
            return "Minor corner blunting, barely visible"
        elif score >= 8.0:
            return "Light corner wear, still presentable"
        elif score >= 7.0:
            return "Noticeable corner wear"
        else:
            return "Significant corner damage"
    
    def _generate_spine_notes(self, score: float) -> str:
        if score >= 9.5:
            return "Spine is tight with no stress marks"
        elif score >= 9.0:
            return "Minimal spine stress, very tight"
        elif score >= 8.0:
            return "Light spine wear, still tight"
        elif score >= 7.0:
            return "Some spine rolling or stress"
        else:
            return "Noticeable spine wear or roll"
    
    def _generate_page_notes(self, score: float) -> str:
        if score >= 9.5:
            return "Pages are white and supple"
        elif score >= 9.0:
            return "Off-white pages, excellent quality"
        elif score >= 8.0:
            return "Cream to off-white pages"
        elif score >= 7.0:
            return "Light tan pages"
        else:
            return "Yellowing or browning pages"
    
    def _generate_centering_notes(self, score: float) -> str:
        if score >= 9.5:
            return "Well-centered front and back"
        elif score >= 9.0:
            return "Slightly off-center, not distracting"
        elif score >= 8.0:
            return "Noticeable off-center but acceptable"
        elif score >= 7.0:
            return "Significant off-center"
        else:
            return "Poorly centered"
    
    def _generate_defects(self, grade: float) -> list:
        """Generate realistic defect list based on grade"""
        all_defects = [
            "minor color break on spine",
            "small crease on back cover",
            "light foxing on interior pages",
            "minor tear on cover edge",
            "small stain on back cover",
            "subscription crease",
            "minor spine tick",
            "light fingerprint on cover",
            "production defect: miscut",
            "minor bindery chip"
        ]
        
        if grade >= 9.6:
            return []
        elif grade >= 9.0:
            return random.sample(all_defects, 1)
        elif grade >= 8.0:
            return random.sample(all_defects, 2)
        elif grade >= 7.0:
            return random.sample(all_defects, 3)
        else:
            return random.sample(all_defects, 4)
