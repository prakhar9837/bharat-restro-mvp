"""Main CLI interface using Typer."""

import asyncio
from pathlib import Path
from typing import Optional

import typer
from typing_extensions import Annotated

from . import (
    config, log, seed, discover, fetch, parse, normalize, 
    geocode, resolve, validate, persist, export, eval as evaluation
)
from .extract.router import route_and_extract
from .log import logger
from .utils import Timer

app = typer.Typer(
    name="bharat-resto",
    help="Indian Restaurant Data Extraction Pipeline",
    add_completion=False
)


@app.command()
def run(
    city: Annotated[str, typer.Option(help="City slug (blr, del, mum, etc.)")] = "blr",
    limit: Annotated[Optional[int], typer.Option(help="Limit number of restaurants to process")] = None,
    llm: Annotated[bool, typer.Option("--llm/--no-llm", help="Enable/disable LLM extraction")] = True,
    seed_file: Annotated[Optional[Path], typer.Option(help="Use CSV file instead of OSM")] = None,
    concurrency: Annotated[int, typer.Option(help="Concurrent fetching")] = 4,
    data_dir: Annotated[Optional[Path], typer.Option(help="Override data directory")] = None,
    db_path: Annotated[Optional[Path], typer.Option(help="Override database path")] = None,
) -> None:
    """Run the complete end-to-end pipeline."""
    
    logger.info("Starting end-to-end pipeline", city=city, limit=limit, llm_enabled=llm)
    
    # Update configuration
    if data_dir:
        config.settings.data_dir = data_dir
    if db_path:
        config.settings.db_path = db_path
    
    config.settings.llm_enabled = llm
    
    # Initialize database
    persist.db_manager.init_db()
    
    try:
        with Timer("end_to_end_pipeline") as timer:
            # Run async pipeline
            asyncio.run(_run_pipeline(city, limit, seed_file, concurrency))
        
        logger.info("Pipeline completed successfully", duration=timer.elapsed)
        
        # Print summary
        typer.echo(f"\n✅ Pipeline completed in {timer.elapsed:.1f} seconds")
        typer.echo(f"📊 Check exports/ directory for results")
        typer.echo(f"🗄️  Database: {config.settings.db_path}")
        
    except Exception as e:
        logger.error("Pipeline failed", error=str(e))
        typer.echo(f"❌ Pipeline failed: {e}", err=True)
        raise typer.Exit(1)


async def _run_pipeline(
    city: str, 
    limit: Optional[int], 
    seed_file: Optional[Path], 
    concurrency: int
) -> None:
    """Internal async pipeline runner."""
    
    # Step 1: Seed
    logger.info("Step 1: Seeding restaurant data")
    if seed_file:
        restaurants = seed.seed_from_file(seed_file)
    else:
        restaurants = await seed.seed_city(city, limit=limit)
    
    if not restaurants:
        logger.warning("No restaurants found from seeding")
        return
    
    logger.info("Seeding completed", restaurants_count=len(restaurants))
    
    # Step 2: Discover websites
    logger.info("Step 2: Discovering websites")
    restaurants = discover.discover_websites(restaurants)
    
    # Step 3: Fetch content for restaurants with websites
    logger.info("Step 3: Fetching web content")
    urls_to_fetch = [r["website"] for r in restaurants if r.get("website")]
    
    if urls_to_fetch:
        fetch_results = await fetch.fetch_urls(urls_to_fetch, concurrency=concurrency)
        logger.info("Fetching completed", urls_fetched=len(fetch_results))
    else:
        logger.info("No URLs to fetch")
    
    # Step 4: Parse content and extract data
    logger.info("Step 4: Parsing content and extracting data")
    
    processed_restaurants = []
    
    for restaurant in restaurants:
        try:
            # Parse content if we have it
            chunks = []
            website = restaurant.get("website")
            
            if website and website in fetch_results:
                content, metadata = fetch_results[website]
                if content:
                    content_type = metadata.get("content_type", "text/html")
                    parser = parse.ContentParser()
                    chunks = parser.parse_content(content, content_type, website)
            
            # Extract structured data
            if chunks:
                extraction_results = await route_and_extract(chunks)
            else:
                # No content to extract from, use only seed data
                extraction_results = {}
            
            # Build restaurant data
            restaurant_data = _build_restaurant_data(restaurant, extraction_results)
            
            # Normalize data
            normalized_data = normalize.normalize_restaurant_data(restaurant_data)
            
            # Geocode if needed
            if not normalized_data.get("lat") or not normalized_data.get("lon"):
                address = normalized_data.get("address_full")
                if address:
                    lat, lon = await geocode.geocode_address(address)
                    if lat and lon:
                        normalized_data["lat"] = lat
                        normalized_data["lon"] = lon
            
            # Validate data
            valid, issues = validate.validate_restaurant_data(normalized_data)
            if not valid:
                logger.warning("Validation failed", restaurant=restaurant.get("name"), issues=issues)
            
            # Resolve entity
            restaurant_id = resolve.resolve_restaurant_entity(normalized_data)
            normalized_data["restaurant_id"] = restaurant_id
            
            # Build provenance records
            provenance_records = _build_provenance_records(extraction_results, website)
            
            # Persist to database
            persist.db_manager.upsert_restaurant(normalized_data, provenance_records)
            
            processed_restaurants.append(restaurant_id)
            
        except Exception as e:
            logger.error("Failed to process restaurant", restaurant=restaurant.get("name"), error=str(e))
            continue
    
    logger.info("Processing completed", processed_count=len(processed_restaurants))
    
    # Step 5: Export results
    logger.info("Step 5: Exporting results")
    export.export_all_formats()
    
    logger.info("Pipeline completed successfully")


def _build_restaurant_data(seed_data: dict, extraction_results: dict) -> dict:
    """Build restaurant data from seed and extraction results."""
    
    restaurant_data = {
        "canonical_name": seed_data.get("name", ""),
    }
    
    # Use seed data as base
    for field in ["lat", "lon", "phone", "website"]:
        value = seed_data.get(field)
        if value:
            restaurant_data[field] = value
    
    # Override/enhance with extraction results
    if "address" in extraction_results:
        addr_result = extraction_results["address"]
        if addr_result.get("value"):
            restaurant_data["address_full"] = addr_result["value"].get("full")
            restaurant_data["pincode"] = addr_result["value"].get("pincode")
    
    if "phone" in extraction_results:
        phone_result = extraction_results["phone"]
        if phone_result.get("value"):
            restaurant_data["phone"] = phone_result["value"]
    
    if "hours" in extraction_results:
        hours_result = extraction_results["hours"]
        if hours_result.get("value"):
            restaurant_data["hours"] = hours_result["value"]
    
    if "cuisines" in extraction_results:
        cuisines_result = extraction_results["cuisines"]
        if cuisines_result.get("value"):
            restaurant_data["cuisines"] = cuisines_result["value"]
    
    # Use structured data from seed if available
    if "cuisines_structured" in seed_data:
        restaurant_data["cuisines"] = seed_data["cuisines_structured"]
    
    if "hours_structured" in seed_data:
        restaurant_data["hours"] = seed_data["hours_structured"]
    
    return restaurant_data


def _build_provenance_records(extraction_results: dict, source_url: Optional[str]) -> list:
    """Build provenance records from extraction results."""
    
    provenance_records = []
    
    for field, result in extraction_results.items():
        if result.get("value") is not None:
            # Map field names
            field_mapping = {
                "address": "address_full",
                "phone": "phone", 
                "hours": "hours",
                "cuisines": "cuisines"
            }
            
            db_field = field_mapping.get(field, field)
            value = result["value"]
            
            # Serialize complex values
            if isinstance(value, (dict, list)):
                import json
                value_str = json.dumps(value)
            else:
                value_str = str(value)
            
            provenance_record = {
                "field": db_field,
                "value": value_str,
                "confidence": result.get("confidence", 0.0),
                "source_url": source_url,
                "content_hash": None,  # Could add content hash
                "model_name": config.settings.ollama_model if config.settings.llm_enabled else "regex",
                "model_version": "1.0",
            }
            
            provenance_records.append(provenance_record)
    
    return provenance_records


@app.command()
def seed_cmd(
    city: Annotated[str, typer.Option(help="City slug")] = "blr",
    limit: Annotated[Optional[int], typer.Option(help="Limit number of results")] = None,
    seed_file: Annotated[Optional[Path], typer.Option(help="Use CSV file instead of OSM")] = None,
) -> None:
    """Seed restaurant data from OSM or file."""
    
    logger.info("Starting seeding", city=city)
    
    try:
        if seed_file:
            restaurants = seed.seed_from_file(seed_file)
        else:
            restaurants = asyncio.run(seed.seed_city(city, limit=limit))
        
        typer.echo(f"✅ Seeded {len(restaurants)} restaurants")
        
        # Print sample
        if restaurants:
            typer.echo("\nSample restaurant:")
            sample = restaurants[0]
            typer.echo(f"  Name: {sample.get('name')}")
            typer.echo(f"  Location: {sample.get('lat')}, {sample.get('lon')}")
            typer.echo(f"  Website: {sample.get('website', 'None')}")
        
    except Exception as e:
        logger.error("Seeding failed", error=str(e))
        typer.echo(f"❌ Seeding failed: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def discover_cmd(
    city: Annotated[str, typer.Option(help="City slug")] = "blr",
) -> None:
    """Discover websites for restaurants."""
    
    logger.info("Starting website discovery", city=city)
    
    try:
        # This would need to load previously seeded data
        typer.echo("🔍 Website discovery would run here")
        typer.echo("💡 Use 'run' command for full pipeline")
        
    except Exception as e:
        logger.error("Discovery failed", error=str(e))
        typer.echo(f"❌ Discovery failed: {e}", err=True)
        raise typer.Exit(1)


@app.command() 
def validate_cmd(
    db_path: Annotated[Optional[Path], typer.Option(help="Database path")] = None,
) -> None:
    """Validate restaurant data in database."""
    
    if db_path:
        config.settings.db_path = db_path
    
    logger.info("Starting data validation")
    
    try:
        persist.db_manager.init_db()
        restaurants = persist.db_manager.get_all_restaurants()
        
        valid_count = 0
        total_issues = []
        
        for restaurant in restaurants:
            restaurant_data = restaurant.to_dict()
            valid, issues = validate.validate_restaurant_data(restaurant_data)
            
            if valid:
                valid_count += 1
            else:
                total_issues.extend(issues)
                logger.warning("Validation issues", restaurant_id=restaurant.restaurant_id, issues=issues)
        
        typer.echo(f"✅ Validated {len(restaurants)} restaurants")
        typer.echo(f"📊 Valid: {valid_count}, Invalid: {len(restaurants) - valid_count}")
        
        if total_issues:
            typer.echo(f"⚠️  Total issues found: {len(total_issues)}")
        
    except Exception as e:
        logger.error("Validation failed", error=str(e))
        typer.echo(f"❌ Validation failed: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def export_cmd(
    format: Annotated[str, typer.Option(help="Export format (csv, json, all)")] = "csv",
    limit: Annotated[Optional[int], typer.Option(help="Limit number of records")] = None,
    output: Annotated[Optional[Path], typer.Option(help="Output file path")] = None,
) -> None:
    """Export restaurant data."""
    
    logger.info("Starting data export", format=format)
    
    try:
        persist.db_manager.init_db()
        
        if format == "all":
            results = export.export_all_formats(limit=limit)
            typer.echo("✅ Exported in all formats:")
            for fmt, path in results.items():
                typer.echo(f"  {fmt}: {path}")
        else:
            output_path = export.export_data(format, limit=limit)
            typer.echo(f"✅ Exported to: {output_path}")
        
    except Exception as e:
        logger.error("Export failed", error=str(e))
        typer.echo(f"❌ Export failed: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def eval_cmd(
    gold_file: Annotated[Optional[Path], typer.Option(help="Gold standard CSV file")] = None,
    output: Annotated[Optional[Path], typer.Option(help="Output report path")] = None,
) -> None:
    """Evaluate extraction against gold standard."""
    
    logger.info("Starting evaluation")
    
    try:
        persist.db_manager.init_db()
        
        if output:
            evaluator = evaluation.EvaluationMetrics(gold_file)
            report_path = evaluator.generate_evaluation_report(output)
            typer.echo(f"✅ Evaluation report: {report_path}")
        else:
            evaluation.print_evaluation_summary(gold_file)
        
    except Exception as e:
        logger.error("Evaluation failed", error=str(e))
        typer.echo(f"❌ Evaluation failed: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def status() -> None:
    """Show pipeline status and statistics."""
    
    try:
        persist.db_manager.init_db()
        
        # Get summary stats
        stats = export.export_summary_stats()
        
        typer.echo("\n📊 BHARAT RESTO MVP - STATUS")
        typer.echo("=" * 40)
        
        typer.echo(f"Total Restaurants: {stats['totals']['restaurants']}")
        typer.echo(f"Provenance Records: {stats['totals']['provenance_records']}")
        
        typer.echo("\n📋 Field Coverage:")
        for field, count in stats['field_counts'].items():
            percentage = stats['coverage_percentage'].get(field, 0)
            typer.echo(f"  {field}: {count} ({percentage}%)")
        
        typer.echo(f"\n🗄️  Database: {config.settings.db_path}")
        typer.echo(f"📁 Data Directory: {config.settings.data_dir}")
        typer.echo(f"📤 Export Directory: {config.settings.export_dir}")
        
    except Exception as e:
        logger.error("Status check failed", error=str(e))
        typer.echo(f"❌ Status check failed: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def serve(
    host: Annotated[str, typer.Option(help="Host to bind to")] = "0.0.0.0",
    port: Annotated[int, typer.Option(help="Port to bind to")] = 8000,
    reload: Annotated[bool, typer.Option("--reload/--no-reload", help="Enable auto-reload")] = False,
    workers: Annotated[int, typer.Option(help="Number of workers")] = 1
):
    """Start the FastAPI web server."""
    try:
        from .server import start_server
        typer.echo(f"🚀 Starting FastAPI server on {host}:{port}")
        if reload:
            typer.echo("🔄 Auto-reload enabled")
        start_server(host=host, port=port, reload=reload, workers=workers)
    except ImportError:
        typer.echo("❌ FastAPI dependencies not installed. Run: pip install fastapi uvicorn", err=True)
        raise typer.Exit(1)
    except Exception as e:
        typer.echo(f"❌ Failed to start server: {e}", err=True)
        raise typer.Exit(1)


def main() -> None:
    """Main entry point."""
    try:
        app()
    except KeyboardInterrupt:
        typer.echo("\n👋 Interrupted by user")
        raise typer.Exit(130)


if __name__ == "__main__":
    main()
