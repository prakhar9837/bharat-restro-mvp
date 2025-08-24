"""Evaluation metrics against gold standard dataset."""

import csv
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .config import settings
from .log import logger
from .normalize import normalize_phone
from .persist import db_manager
from .validate import validate_pincode


class EvaluationMetrics:
    """Calculate evaluation metrics against gold standard."""
    
    def __init__(self, gold_file_path: Optional[Path] = None):
        self.gold_file_path = gold_file_path or Path("gold/gold_sample.csv")
        self.gold_data = self._load_gold_data()
    
    def _load_gold_data(self) -> List[Dict[str, any]]:
        """Load gold standard data from CSV."""
        gold_data = []
        
        try:
            with open(self.gold_file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    # Parse JSON fields
                    cuisines = json.loads(row.get("cuisines", "[]"))
                    hours = json.loads(row.get("hours_json", "{}"))
                    
                    gold_record = {
                        "name": row["name"],
                        "phone": row.get("phone"),
                        "address_full": row.get("address_full"),
                        "pincode": row.get("pincode"),
                        "lat": float(row["lat"]) if row.get("lat") else None,
                        "lon": float(row["lon"]) if row.get("lon") else None,
                        "website": row.get("website"),
                        "cuisines": cuisines,
                        "hours": hours,
                    }
                    
                    gold_data.append(gold_record)
            
            logger.info("Loaded gold standard data", records_count=len(gold_data))
            
        except Exception as e:
            logger.error("Failed to load gold standard data", error=str(e))
            raise
        
        return gold_data
    
    def find_matching_restaurant(self, gold_record: Dict[str, any]) -> Optional[Dict[str, any]]:
        """Find extracted restaurant that matches gold record."""
        
        gold_name = gold_record["name"]
        
        # Search by name similarity
        restaurants = db_manager.search_restaurants(name=gold_name, limit=10)
        
        if not restaurants:
            return None
        
        # Find best match by name similarity
        from rapidfuzz import fuzz
        
        best_match = None
        best_score = 0
        
        for restaurant in restaurants:
            score = fuzz.ratio(gold_name.lower(), restaurant.canonical_name.lower())
            if score > best_score:
                best_score = score
                best_match = restaurant.to_dict()
        
        # Only accept matches with high similarity
        if best_score >= 80:  # 80% similarity threshold
            return best_match
        
        return None
    
    def evaluate_field_extraction(self, field: str) -> Dict[str, any]:
        """Evaluate extraction accuracy for a specific field."""
        
        logger.info(f"Evaluating {field} extraction")
        
        true_positives = 0
        false_positives = 0
        false_negatives = 0
        
        field_comparisons = []
        
        for gold_record in self.gold_data:
            extracted_record = self.find_matching_restaurant(gold_record)
            
            gold_value = gold_record.get(field)
            extracted_value = extracted_record.get(field) if extracted_record else None
            
            # Normalize values for comparison
            if field == "phone":
                gold_value = normalize_phone(gold_value) if gold_value else None
                extracted_value = normalize_phone(extracted_value) if extracted_value else None
            elif field == "pincode":
                # Validate pincode format
                gold_value = gold_value if validate_pincode(gold_value)[0] else None
                extracted_value = extracted_value if validate_pincode(extracted_value)[0] else None
            elif field == "cuisines":
                # Compare as sets for cuisines
                gold_value = set(gold_value) if gold_value else set()
                extracted_value = set(extracted_value) if extracted_value else set()
            
            # Record comparison
            comparison = {
                "restaurant_name": gold_record["name"],
                "gold_value": gold_value,
                "extracted_value": extracted_value,
                "match": gold_value == extracted_value if gold_value and extracted_value else False
            }
            field_comparisons.append(comparison)
            
            # Calculate metrics
            has_gold = gold_value is not None and gold_value != "" and gold_value != set()
            has_extracted = extracted_value is not None and extracted_value != "" and extracted_value != set()
            
            if has_gold and has_extracted:
                if field == "cuisines":
                    # For cuisines, consider it correct if any overlap
                    if gold_value & extracted_value:  # Set intersection
                        true_positives += 1
                    else:
                        false_positives += 1
                        false_negatives += 1
                else:
                    if gold_value == extracted_value:
                        true_positives += 1
                    else:
                        false_positives += 1
                        false_negatives += 1
            elif has_gold and not has_extracted:
                false_negatives += 1
            elif not has_gold and has_extracted:
                false_positives += 1
            # Both empty/null = true negative (not counted in precision/recall)
        
        # Calculate metrics
        precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0
        recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0
        f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
        
        # Coverage (how many records have this field extracted)
        total_extracted = sum(1 for comp in field_comparisons if comp["extracted_value"] is not None)
        coverage = total_extracted / len(self.gold_data) if self.gold_data else 0
        
        results = {
            "field": field,
            "precision": round(precision, 3),
            "recall": round(recall, 3),
            "f1_score": round(f1_score, 3),
            "coverage": round(coverage, 3),
            "true_positives": true_positives,
            "false_positives": false_positives,
            "false_negatives": false_negatives,
            "total_gold_records": len(self.gold_data),
            "comparisons": field_comparisons
        }
        
        logger.info(f"{field} evaluation completed", 
                   precision=precision, 
                   recall=recall, 
                   f1_score=f1_score,
                   coverage=coverage)
        
        return results
    
    def evaluate_all_fields(self) -> Dict[str, any]:
        """Evaluate extraction for all fields."""
        
        logger.info("Starting comprehensive evaluation")
        
        fields_to_evaluate = [
            "phone",
            "address_full", 
            "pincode",
            "website",
            "cuisines"
        ]
        
        field_results = {}
        
        for field in fields_to_evaluate:
            try:
                field_results[field] = self.evaluate_field_extraction(field)
            except Exception as e:
                logger.error(f"Failed to evaluate {field}", error=str(e))
                field_results[field] = {
                    "error": str(e),
                    "precision": 0,
                    "recall": 0, 
                    "f1_score": 0,
                    "coverage": 0
                }
        
        # Calculate overall metrics
        overall_precision = sum(r.get("precision", 0) for r in field_results.values()) / len(field_results)
        overall_recall = sum(r.get("recall", 0) for r in field_results.values()) / len(field_results)
        overall_f1 = sum(r.get("f1_score", 0) for r in field_results.values()) / len(field_results)
        overall_coverage = sum(r.get("coverage", 0) for r in field_results.values()) / len(field_results)
        
        # Restaurant-level matching
        matched_restaurants = 0
        for gold_record in self.gold_data:
            if self.find_matching_restaurant(gold_record):
                matched_restaurants += 1
        
        restaurant_coverage = matched_restaurants / len(self.gold_data) if self.gold_data else 0
        
        results = {
            "evaluation_metadata": {
                "gold_file": str(self.gold_file_path),
                "total_gold_records": len(self.gold_data),
                "matched_restaurants": matched_restaurants,
                "restaurant_coverage": round(restaurant_coverage, 3)
            },
            "overall_metrics": {
                "precision": round(overall_precision, 3),
                "recall": round(overall_recall, 3),
                "f1_score": round(overall_f1, 3),
                "coverage": round(overall_coverage, 3)
            },
            "field_metrics": field_results
        }
        
        logger.info("Comprehensive evaluation completed",
                   overall_f1=overall_f1,
                   restaurant_coverage=restaurant_coverage)
        
        return results
    
    def generate_evaluation_report(self, output_path: Optional[Path] = None) -> Path:
        """Generate detailed evaluation report."""
        
        if not output_path:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = settings.export_dir / f"evaluation_report_{timestamp}.json"
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info("Generating evaluation report", output_path=str(output_path))
        
        try:
            results = self.evaluate_all_fields()
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            
            logger.info("Evaluation report generated", output_path=str(output_path))
            
            return output_path
            
        except Exception as e:
            logger.error("Failed to generate evaluation report", error=str(e))
            raise
    
    def print_summary_report(self) -> None:
        """Print evaluation summary to console."""
        
        try:
            results = self.evaluate_all_fields()
            
            print("\n" + "="*60)
            print("BHARAT RESTO MVP - EVALUATION REPORT")
            print("="*60)
            
            print(f"\nGold Standard: {results['evaluation_metadata']['total_gold_records']} restaurants")
            print(f"Matched: {results['evaluation_metadata']['matched_restaurants']} restaurants")
            print(f"Restaurant Coverage: {results['evaluation_metadata']['restaurant_coverage']:.1%}")
            
            print(f"\nOVERALL METRICS:")
            print(f"  Precision: {results['overall_metrics']['precision']:.3f}")
            print(f"  Recall:    {results['overall_metrics']['recall']:.3f}")
            print(f"  F1 Score:  {results['overall_metrics']['f1_score']:.3f}")
            print(f"  Coverage:  {results['overall_metrics']['coverage']:.3f}")
            
            print(f"\nFIELD-SPECIFIC METRICS:")
            print(f"{'Field':<15} {'Precision':<10} {'Recall':<10} {'F1':<10} {'Coverage':<10}")
            print("-" * 55)
            
            for field, metrics in results['field_metrics'].items():
                if 'error' not in metrics:
                    print(f"{field:<15} {metrics['precision']:<10.3f} {metrics['recall']:<10.3f} "
                          f"{metrics['f1_score']:<10.3f} {metrics['coverage']:<10.3f}")
                else:
                    print(f"{field:<15} ERROR: {metrics['error']}")
            
            print("\n" + "="*60)
            
        except Exception as e:
            logger.error("Failed to print summary report", error=str(e))
            print(f"Error generating report: {e}")


# Global evaluator instance
evaluator = EvaluationMetrics()


def evaluate_against_gold(gold_file: Optional[Path] = None) -> Dict[str, any]:
    """Evaluate extraction results against gold standard."""
    if gold_file:
        eval_instance = EvaluationMetrics(gold_file)
    else:
        eval_instance = evaluator
    
    return eval_instance.evaluate_all_fields()


def print_evaluation_summary(gold_file: Optional[Path] = None) -> None:
    """Print evaluation summary to console."""
    if gold_file:
        eval_instance = EvaluationMetrics(gold_file)
    else:
        eval_instance = evaluator
    
    eval_instance.print_summary_report()
