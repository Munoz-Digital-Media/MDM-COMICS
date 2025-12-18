"""
BCW Product Image Verification Script

Generates a report to verify images are matched to correct products.
Compares:
- Catalog data (BCW-SKU, BCW-NAME, MDM-SKU)
- Database product (name, sku)
- S3 image URLs

Run: python -m app.scripts.verify_bcw_images
"""
import asyncio
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
from sqlalchemy import text

from app.core.database import AsyncSessionLocal

logger = logging.getLogger(__name__)

# Path to BCW catalog
CATALOG_PATH = Path("F:/apps/mdm_comics/assets/docs/implementations/bcw_implementation/20251216_mdm_comics_bcw_catalog.xlsx")
OUTPUT_PATH = Path(__file__).parent.parent.parent / "data" / "bcw_image_verification_report.html"


async def generate_verification_report():
    """Generate HTML report for visual verification of BCW product images."""

    print("=" * 60)
    print("BCW PRODUCT IMAGE VERIFICATION")
    print("=" * 60)

    # Load catalog
    try:
        df = pd.read_excel(CATALOG_PATH, header=1)
        df = df.dropna(subset=['BCW-SKU', 'MDM-SKU'])
        catalog_products = df[['BCW-SKU', 'BCW-NAME', 'MDM-SKU', 'URL']].to_dict('records')
        print(f"Loaded {len(catalog_products)} products from catalog")
    except Exception as e:
        print(f"ERROR: Failed to read catalog: {e}")
        return

    # Query database
    async with AsyncSessionLocal() as db:
        # Get products with their images
        result = await db.execute(text("""
            SELECT
                p.id,
                p.sku,
                p.name,
                p.image_url,
                p.images
            FROM products p
            WHERE p.sku LIKE 'MDM-%'
            ORDER BY p.sku
        """))
        db_products = {row.sku: dict(row._mapping) for row in result.fetchall()}
        print(f"Found {len(db_products)} BCW products in database")

        # Get all product images
        try:
            img_result = await db.execute(text("""
                SELECT product_sku, image_url, position
                FROM product_images
                WHERE source = 'bcw'
                ORDER BY product_sku, position
            """))
            all_images = {}
            for row in img_result.fetchall():
                sku = row.product_sku
                if sku not in all_images:
                    all_images[sku] = []
                all_images[sku].append(row.image_url)
            print(f"Found images for {len(all_images)} products")
        except Exception as e:
            print(f"Note: product_images table not available: {e}")
            all_images = {}

    # Build verification report
    report_data = []
    mismatches = []
    missing_images = []
    missing_products = []

    for catalog in catalog_products:
        bcw_sku = catalog['BCW-SKU']
        bcw_name = catalog['BCW-NAME']
        mdm_sku = catalog['MDM-SKU']
        bcw_url = catalog.get('URL', '')

        db_product = db_products.get(mdm_sku)
        images = all_images.get(mdm_sku, [])

        status = "OK"
        notes = []

        if not db_product:
            status = "MISSING_PRODUCT"
            missing_products.append(mdm_sku)
            notes.append("Product not in database")
        else:
            cover_url = db_product.get('image_url')
            db_images = db_product.get('images') or []
            db_name = db_product.get('name', '')

            if not cover_url and not images:
                status = "NO_IMAGE"
                missing_images.append(mdm_sku)
                notes.append("No images found")

            # Check if names are similar (basic sanity check)
            if db_name and bcw_name:
                bcw_lower = bcw_name.lower()
                db_lower = db_name.lower()
                # Check if key words match
                bcw_words = set(bcw_lower.split())
                db_words = set(db_lower.split())
                common = bcw_words & db_words
                if len(common) < 2 and len(bcw_words) > 2:
                    notes.append(f"Name mismatch? Catalog: '{bcw_name}' vs DB: '{db_name}'")

        # Combine all image sources
        all_product_images = []
        if db_product:
            if db_product.get('image_url'):
                all_product_images.append(db_product['image_url'])
            if db_product.get('images'):
                all_product_images.extend(db_product['images'])
        # Add from product_images table
        all_product_images.extend(images)
        # Deduplicate
        all_product_images = list(dict.fromkeys(all_product_images))

        report_data.append({
            "bcw_sku": bcw_sku,
            "mdm_sku": mdm_sku,
            "catalog_name": bcw_name,
            "db_name": db_product.get('name', 'N/A') if db_product else 'N/A',
            "cover_image": db_product.get('image_url', '') if db_product else '',
            "image_count": len(all_product_images),
            "all_images": all_product_images,
            "bcw_url": bcw_url,
            "status": status,
            "notes": "; ".join(notes) if notes else "",
        })

    # Generate HTML report
    html = generate_html_report(report_data)

    # Write report
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\n{'=' * 60}")
    print("VERIFICATION SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total products in catalog: {len(catalog_products)}")
    print(f"Products in database:      {len(db_products)}")
    print(f"Products with images:      {len(all_images)}")
    print(f"Missing from database:     {len(missing_products)}")
    print(f"Missing images:            {len(missing_images)}")
    print(f"\nReport saved to: {OUTPUT_PATH}")
    print(f"\nOpen the HTML file in a browser to visually verify image matches.")

    return report_data


def generate_html_report(data):
    """Generate HTML report with images for visual verification."""

    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>BCW Product Image Verification Report</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 20px; background: #1a1a1a; color: #e0e0e0; }}
        h1 {{ color: #f97316; }}
        .summary {{ background: #2a2a2a; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
        .summary h2 {{ margin-top: 0; color: #f97316; }}
        .filters {{ margin-bottom: 20px; }}
        .filters button {{ padding: 8px 16px; margin-right: 8px; border: none; border-radius: 4px; cursor: pointer; background: #333; color: #e0e0e0; }}
        .filters button:hover {{ background: #444; }}
        .filters button.active {{ background: #f97316; color: white; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #333; vertical-align: top; }}
        th {{ background: #2a2a2a; color: #f97316; position: sticky; top: 0; }}
        tr:hover {{ background: #2a2a2a; }}
        .status-OK {{ color: #22c55e; }}
        .status-NO_IMAGE {{ color: #eab308; }}
        .status-MISSING_PRODUCT {{ color: #ef4444; }}
        .image-cell {{ width: 150px; }}
        .image-cell img {{ max-width: 140px; max-height: 140px; border-radius: 4px; cursor: pointer; }}
        .image-cell img:hover {{ transform: scale(1.1); }}
        .sku {{ font-family: monospace; font-size: 12px; color: #888; }}
        .name {{ font-weight: 500; }}
        .notes {{ font-size: 12px; color: #eab308; }}
        .bcw-link {{ font-size: 12px; color: #60a5fa; }}
        .all-images {{ display: flex; flex-wrap: wrap; gap: 5px; margin-top: 5px; }}
        .all-images img {{ width: 50px; height: 50px; object-fit: cover; border-radius: 4px; cursor: pointer; }}
        .hidden {{ display: none; }}
    </style>
</head>
<body>
    <h1>BCW Product Image Verification Report</h1>
    <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

    <div class="summary">
        <h2>Summary</h2>
        <p>Total products: <strong>{len(data)}</strong></p>
        <p>With images: <strong>{len([d for d in data if d['cover_image']])}</strong></p>
        <p>Missing images: <strong>{len([d for d in data if d['status'] == 'NO_IMAGE'])}</strong></p>
        <p>Missing from DB: <strong>{len([d for d in data if d['status'] == 'MISSING_PRODUCT'])}</strong></p>
    </div>

    <div class="filters">
        <button class="active" onclick="filterRows('all')">All ({len(data)})</button>
        <button onclick="filterRows('OK')">OK ({len([d for d in data if d['status'] == 'OK'])})</button>
        <button onclick="filterRows('NO_IMAGE')">No Image ({len([d for d in data if d['status'] == 'NO_IMAGE'])})</button>
        <button onclick="filterRows('MISSING_PRODUCT')">Missing Product ({len([d for d in data if d['status'] == 'MISSING_PRODUCT'])})</button>
    </div>

    <table>
        <thead>
            <tr>
                <th>Image</th>
                <th>SKU / Name</th>
                <th>DB Name</th>
                <th>Status</th>
                <th>Notes</th>
            </tr>
        </thead>
        <tbody>
"""

    for item in data:
        cover_img = item['cover_image']
        img_html = f'<img src="{cover_img}" alt="{item["mdm_sku"]}" onclick="window.open(this.src)">' if cover_img else '<span style="color:#666">No image</span>'

        # Additional images
        all_imgs_html = ""
        if item['all_images'] and len(item['all_images']) > 1:
            all_imgs_html = '<div class="all-images">'
            for img_url in item['all_images'][1:5]:  # Show up to 4 additional
                all_imgs_html += f'<img src="{img_url}" onclick="window.open(this.src)">'
            if len(item['all_images']) > 5:
                all_imgs_html += f'<span style="color:#888">+{len(item["all_images"])-5} more</span>'
            all_imgs_html += '</div>'

        bcw_link = f'<a href="{item["bcw_url"]}" target="_blank" class="bcw-link">View on BCW</a>' if item['bcw_url'] else ''

        html += f"""
            <tr class="row-{item['status']}" data-status="{item['status']}">
                <td class="image-cell">{img_html}{all_imgs_html}</td>
                <td>
                    <div class="sku">BCW: {item['bcw_sku']}</div>
                    <div class="sku">MDM: {item['mdm_sku']}</div>
                    <div class="name">{item['catalog_name']}</div>
                    {bcw_link}
                </td>
                <td>{item['db_name']}</td>
                <td class="status-{item['status']}">{item['status']}</td>
                <td class="notes">{item['notes']}</td>
            </tr>
"""

    html += """
        </tbody>
    </table>

    <script>
        function filterRows(status) {
            document.querySelectorAll('.filters button').forEach(b => b.classList.remove('active'));
            event.target.classList.add('active');

            document.querySelectorAll('tbody tr').forEach(row => {
                if (status === 'all' || row.dataset.status === status) {
                    row.classList.remove('hidden');
                } else {
                    row.classList.add('hidden');
                }
            });
        }
    </script>
</body>
</html>
"""

    return html


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(generate_verification_report())
