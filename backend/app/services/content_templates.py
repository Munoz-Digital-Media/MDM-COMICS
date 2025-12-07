"""
Content Template Service

v1.5.0: Outreach System - Jinja2 template rendering for marketing content
"""
from pathlib import Path
from typing import Dict, Any, Optional
import logging

from app.core.config import settings

logger = logging.getLogger(__name__)

# Try to import Jinja2, but gracefully handle if not installed
try:
    from jinja2 import Environment, FileSystemLoader, select_autoescape, TemplateNotFound
    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False
    logger.warning("jinja2 not installed - template rendering will use fallbacks")


class ContentTemplateService:
    """Render marketing content from templates."""

    def __init__(self):
        template_dir = Path(settings.MARKETING_TEMPLATE_DIR)

        if JINJA2_AVAILABLE and template_dir.exists():
            self.env = Environment(
                loader=FileSystemLoader(str(template_dir)),
                autoescape=select_autoescape(['html', 'xml']),
            )
            self.templates_available = True
        else:
            if not JINJA2_AVAILABLE:
                logger.warning("Jinja2 not available")
            elif not template_dir.exists():
                logger.warning(f"Template directory not found: {template_dir}")
            self.env = None
            self.templates_available = False

    async def render_price_mover_post(
        self,
        mover: Any,
        platform: str = "bluesky",
    ) -> str:
        """Render price mover for social posting."""
        if not self.templates_available:
            return self._fallback_price_mover(mover)

        try:
            template = self.env.get_template(f'social/{platform}_price_winner.txt')
            return template.render(mover=mover)
        except Exception as e:
            logger.warning(f"Template render failed: {e}")
            return self._fallback_price_mover(mover)

    async def render_new_arrival_post(
        self,
        arrival: Any,
        platform: str = "bluesky",
    ) -> str:
        """Render new arrival for social posting."""
        if not self.templates_available:
            return self._fallback_new_arrival(arrival)

        try:
            template = self.env.get_template(f'social/{platform}_new_arrival.txt')
            return template.render(arrival=arrival)
        except Exception as e:
            logger.warning(f"Template render failed: {e}")
            return self._fallback_new_arrival(arrival)

    async def render_newsletter_section(
        self,
        section: str,
        data: Dict[str, Any],
    ) -> str:
        """Render newsletter section HTML."""
        if not self.templates_available:
            return f"<!-- Template for {section} not available -->"

        try:
            template = self.env.get_template(f'newsletter/{section}.html')
            return template.render(**data)
        except Exception as e:
            logger.error(f"Newsletter section render failed: {e}")
            return f"<!-- Section {section} failed to render -->"

    def _fallback_price_mover(self, mover: Any) -> str:
        """Static fallback when template fails."""
        direction = "up" if getattr(mover, 'change_percent', 0) > 0 else "down"
        emoji = "📈" if direction == "up" else "📉"
        name = getattr(mover, 'name', 'Unknown Item')
        price_old = getattr(mover, 'price_old', 0)
        price_new = getattr(mover, 'price_new', 0)
        change_pct = getattr(mover, 'change_percent', 0)

        return (
            f"{emoji} {name}\n"
            f"${price_old:.2f} -> ${price_new:.2f} "
            f"({change_pct:+.1f}%)\n\n"
            f"#comics #collectibles #mdmcomics"
        )

    def _fallback_new_arrival(self, arrival: Any) -> str:
        """Static fallback for new arrivals."""
        name = getattr(arrival, 'name', 'New Item')
        price = getattr(arrival, 'price', None)
        category = getattr(arrival, 'category', 'collectible')

        price_str = f" - ${price:.2f}" if price else ""

        return (
            f"🆕 Just In: {name}{price_str}\n\n"
            f"Shop now at mdmcomics.com\n\n"
            f"#{category.replace(' ', '')} #newarrivals #mdmcomics"
        )
