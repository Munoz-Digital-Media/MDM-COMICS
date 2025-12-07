"""
Content AI Service

v1.5.0: Outreach System - AI-powered content enhancement with circuit breaker
"""
import logging
import time
from dataclasses import dataclass
from typing import Optional

from app.core.config import settings

logger = logging.getLogger(__name__)


@dataclass
class ContentResult:
    content: str
    source: str  # "ai" or "template"
    fallback_used: bool = False
    model: Optional[str] = None
    error: Optional[str] = None


class CircuitBreaker:
    """Simple circuit breaker for external API calls."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = "closed"  # closed, open, half-open

    def can_execute(self) -> bool:
        if self.state == "closed":
            return True

        if self.state == "open":
            # Check if recovery timeout has passed
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = "half-open"
                return True
            return False

        # half-open: allow one request
        return True

    def record_success(self):
        self.failure_count = 0
        self.state = "closed"

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.failure_count >= self.failure_threshold:
            self.state = "open"
            logger.warning(f"Circuit breaker opened after {self.failure_count} failures")


class ContentAIService:
    """AI-powered content enhancement with fallback."""

    def __init__(self):
        self.circuit_breaker = CircuitBreaker()
        self._client = None

    def _get_client(self):
        """Lazy load OpenAI client."""
        if self._client is None:
            try:
                from openai import AsyncOpenAI
                self._client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
            except ImportError:
                logger.error("openai package not installed")
                return None
        return self._client

    async def enhance_blurb(
        self,
        base_content: str,
        tone: str = "engaging",
        max_length: int = 280,
        context: Optional[str] = None,
    ) -> ContentResult:
        """
        Enhance content with AI, with feature flag and circuit breaker.

        Falls back to base_content if AI is unavailable or fails.
        """
        # Feature flag check
        if not settings.MARKETING_AI_ENHANCEMENT_ENABLED:
            logger.debug("AI enhancement disabled by feature flag")
            return ContentResult(
                content=base_content,
                source="template",
                fallback_used=True,
                error="AI enhancement disabled",
            )

        # API key check
        if not settings.OPENAI_API_KEY:
            logger.warning("OPENAI_API_KEY not set, using template fallback")
            return ContentResult(
                content=base_content,
                source="template",
                fallback_used=True,
                error="API key not configured",
            )

        # Circuit breaker check
        if not self.circuit_breaker.can_execute():
            logger.warning("Circuit breaker open, using fallback")
            return ContentResult(
                content=base_content,
                source="template",
                fallback_used=True,
                error="Circuit breaker open",
            )

        client = self._get_client()
        if not client:
            return ContentResult(
                content=base_content,
                source="template",
                fallback_used=True,
                error="OpenAI client unavailable",
            )

        try:
            system_prompt = f"""You are a social media copywriter for MDM Comics, a comic book and collectibles shop.
Write in a {tone} tone. Keep posts under {max_length} characters.
Use relevant hashtags like #comics #collectibles #funko when appropriate.
Never use emojis unless specifically requested."""

            user_prompt = base_content
            if context:
                user_prompt = f"{context}\n\n{base_content}"

            response = await client.chat.completions.create(
                model=settings.OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Enhance this post:\n\n{user_prompt}"},
                ],
                max_tokens=150,
                temperature=0.7,
            )

            enhanced = response.choices[0].message.content.strip()

            # Validate length
            if len(enhanced) > max_length:
                enhanced = enhanced[:max_length - 3] + "..."

            self.circuit_breaker.record_success()

            return ContentResult(
                content=enhanced,
                source="ai",
                model=settings.OPENAI_MODEL,
            )

        except Exception as e:
            logger.error(f"OpenAI API error: {e}")
            self.circuit_breaker.record_failure()

            return ContentResult(
                content=base_content,
                source="template",
                fallback_used=True,
                error=str(e),
            )

    async def generate_newsletter_section(
        self,
        section_type: str,
        data: dict,
    ) -> ContentResult:
        """Generate newsletter section content."""
        if not settings.MARKETING_AI_ENHANCEMENT_ENABLED:
            return ContentResult(
                content="",
                source="template",
                fallback_used=True,
                error="AI enhancement disabled",
            )

        # Build prompt based on section type
        prompts = {
            "price_winners": "Write a brief, exciting intro for our weekly price winners section. These items have increased in value.",
            "price_losers": "Write a brief intro for items that have decreased in value. Position it as a buying opportunity.",
            "new_arrivals": "Write an engaging intro for our new arrivals section.",
            "weekly_recap": "Write a brief weekly recap intro for comics and collectibles news.",
        }

        prompt = prompts.get(section_type, f"Write a brief intro for the {section_type} section.")

        return await self.enhance_blurb(
            base_content=prompt,
            tone="professional yet friendly",
            max_length=500,
            context=f"Data context: {data}",
        )
