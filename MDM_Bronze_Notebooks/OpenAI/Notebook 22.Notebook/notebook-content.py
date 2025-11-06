# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import pandas as pd
from ydata_profiling import ProfileReport
import json
import numpy as np

def analyze_columns_for_enrichment(df, profile_report=None):
    """
    Analyze DataFrame columns to identify which need enrichment.
    
    Args:
        df: pandas DataFrame
        profile_report: Optional pre-generated ProfileReport object
    
    Returns:
        dict: Summary of columns requiring enrichment with reasoning
    """
    
    # Generate profile if not provided
    if profile_report is None:
        profile_report = ProfileReport(df, minimal=True, explorative=True)
    
    # Extract variable information
    description = profile_report.get_description()
    variables = description.variables
    
    enrichment_candidates = {}
    
    for col_name, col_info in variables.items():
        issues = []
        metrics = {}
        
        # Get column type
        col_type = col_info.get('type', 'Unknown')
        metrics['type'] = col_type
        
        # 1. Check for inconsistent formatting (high uniqueness but patterns)
        if col_type in ['Categorical', 'Text']:
            n_unique = col_info.get('n_unique', 0)
            n_total = col_info.get('count', 0)
            uniqueness_ratio = n_unique / n_total if n_total > 0 else 0
            
            metrics['unique_values'] = n_unique
            metrics['uniqueness_ratio'] = round(uniqueness_ratio, 3)
            
            # High uniqueness might indicate addresses, emails, names, etc.
            if 0.5 < uniqueness_ratio < 1.0 and n_unique > 10:
                issues.append(f"High uniqueness ({uniqueness_ratio:.1%}) suggests varied formats")
        
        # 2. Check for missing values
        n_missing = col_info.get('n_missing', 0)
        missing_pct = col_info.get('p_missing', 0)
        
        metrics['missing_count'] = n_missing
        metrics['missing_percentage'] = round(missing_pct * 100, 2)
        
        if missing_pct > 0.1:  # More than 10% missing
            issues.append(f"High missing values ({missing_pct:.1%}) may need imputation")
        
        # 3. Check for potential standardization issues
        if col_type == 'Text':
            # Look for mixed case or whitespace issues (if available in stats)
            value_lengths = col_info.get('value_counts_without_nan', {})
            
            if len(value_lengths) > 0:
                # Check if same values with different cases/spaces exist
                unique_values = list(value_lengths.keys())[:20]  # Sample first 20
                normalized = [str(v).strip().lower() for v in unique_values]
                
                if len(set(normalized)) < len(unique_values):
                    issues.append("Potential case/whitespace inconsistencies detected")
        
        # 4. Detect specific column types by name patterns
        col_lower = col_name.lower()
        
        # Address-like columns
        if any(keyword in col_lower for keyword in ['address', 'street', 'city', 'state', 'zip', 'postal', 'location']):
            issues.append("Address-type field: likely needs format standardization")
            metrics['suspected_type'] = 'Address'
        
        # Email columns
        elif any(keyword in col_lower for keyword in ['email', 'mail']):
            issues.append("Email field: should validate format and domain")
            metrics['suspected_type'] = 'Email'
        
        # Phone columns
        elif any(keyword in col_lower for keyword in ['phone', 'mobile', 'tel', 'contact']):
            issues.append("Phone number: needs format standardization (country code, separators)")
            metrics['suspected_type'] = 'Phone'
        
        # Name columns
        elif any(keyword in col_lower for keyword in ['name', 'firstname', 'lastname', 'fullname']):
            issues.append("Name field: may need case standardization, title handling")
            metrics['suspected_type'] = 'Name'
        
        # Date columns
        elif col_type == 'DateTime' or any(keyword in col_lower for keyword in ['date', 'time', 'timestamp']):
            issues.append("Date/time field: verify consistent format and timezone")
            metrics['suspected_type'] = 'DateTime'
        
        # Company/Organization
        elif any(keyword in col_lower for keyword in ['company', 'organization', 'business', 'firm']):
            issues.append("Company name: may need standardization (Inc., LLC, etc.)")
            metrics['suspected_type'] = 'Company'
        
        # 5. Check for outliers in numeric columns
        if col_type == 'Numeric':
            has_outliers = col_info.get('n_outliers', 0) > 0
            if has_outliers:
                issues.append(f"Contains {col_info.get('n_outliers', 0)} outliers that may need review")
        
        # 6. Check data quality warnings from profiling
        if 'alerts' in col_info:
            for alert in col_info['alerts']:
                issues.append(f"Quality issue: {alert}")
        
        # Add to candidates if issues found
        if issues:
            enrichment_candidates[col_name] = {
                'metrics': metrics,
                'issues': issues,
                'priority': len(issues)  # Simple priority based on issue count
            }
    
    return enrichment_candidates


def format_for_gpt_analysis(enrichment_candidates, df_sample=None):
    """
    Format the enrichment candidates into a prompt for GPT analysis.
    
    Args:
        enrichment_candidates: Output from analyze_columns_for_enrichment
        df_sample: Optional sample of the DataFrame for context
    
    Returns:
        str: Formatted prompt for GPT
    """
    
    prompt = """I have analyzed a dataset and identified the following columns that may require enrichment/standardization. Please review each column and provide recommendations on whether it should be added to the 'required_enrichment' list and what specific enrichment steps would be beneficial.

Dataset Context:
"""
    
    if df_sample is not None:
        prompt += f"- Total rows: {len(df_sample)}\n"
        prompt += f"- Total columns: {len(df_sample.columns)}\n"
        prompt += f"- Sample preview:\n{df_sample.head(3).to_string()}\n\n"
    
    prompt += "\nColumns Flagged for Potential Enrichment:\n\n"
    
    # Sort by priority
    sorted_candidates = sorted(
        enrichment_candidates.items(),
        key=lambda x: x[1]['priority'],
        reverse=True
    )
    
    for col_name, details in sorted_candidates:
        prompt += f"### Column: '{col_name}'\n"
        prompt += f"**Priority Score**: {details['priority']}\n\n"
        
        prompt += "**Metrics**:\n"
        for metric, value in details['metrics'].items():
            prompt += f"- {metric}: {value}\n"
        
        prompt += "\n**Issues Detected**:\n"
        for issue in details['issues']:
            prompt += f"- {issue}\n"
        
        prompt += "\n---\n\n"
    
    prompt += """
For each column above, please provide:
1. **Should it be enriched?** (Yes/No)
2. **Enrichment type**: What kind of enrichment? (e.g., format standardization, validation, deduplication, imputation)
3. **Specific actions**: What exact steps should be taken?
4. **Priority level**: High/Medium/Low
5. **Potential tools/methods**: Suggest libraries or approaches

Format your response as a JSON object with column names as keys."""
    
    return prompt


# Example usage
if __name__ == "__main__":
    # Sample data with enrichment needs
    sample_data = {
        'customer_id': [1, 2, 3, 4, 5],
        'name': ['john doe', 'JANE SMITH', 'Bob  Johnson', 'alice WILLIAMS', 'Charlie Brown'],
        'email': ['john@email.com', 'jane@EMAIL.COM', 'bob@', None, 'charlie@domain.com'],
        'address': ['123 Main St', '456 elm street apt 2', '789 Oak Avenue', None, '321 pine st.'],
        'phone': ['555-1234', '(555) 5678', '5559012', '555.3456', None],
        'amount': [100, 200, 150, 10000, 175]  # Has outlier
    }
    
    df = pd.DataFrame(sample_data)
    
    # Generate profile
    profile = ProfileReport(df, minimal=True, title="Sample Dataset")
    
    # Analyze for enrichment
    candidates = analyze_columns_for_enrichment(df, profile)
    
    # Print results
    print("=" * 80)
    print("ENRICHMENT CANDIDATES DETECTED")
    print("=" * 80)
    print(json.dumps(candidates, indent=2))
    
    # Format for GPT
    print("\n\n" + "=" * 80)
    print("GPT ANALYSIS PROMPT")
    print("=" * 80)
    gpt_prompt = format_for_gpt_analysis(candidates, df)
    print(gpt_prompt)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
