"""
Visualization utilities for exploratory analysis.

This module provides reusable plotting functions for:
- Time series analysis
- Comparative visualizations
- Distribution plots
- Correlation heatmaps
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from typing import Optional, List


def plot_dead_money_trend(df: pd.DataFrame, title: str = "Dead Money Over Time") -> None:
    """
    Plot dead money trends over time.
    
    Args:
        df: DataFrame with year and dead_money columns
        title: Plot title
    """
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df, x='year', y='dead_money')
    plt.title(title)
    plt.xlabel('Year')
    plt.ylabel('Dead Money ($M)')
    plt.show()


def plot_team_comparison(df: pd.DataFrame, metric: str, top_n: int = 10) -> None:
    """
    Compare teams on a specific metric.
    
    Args:
        df: DataFrame with team and metric data
        metric: Column name to compare
        top_n: Number of teams to show
    """
    top_teams = df.nlargest(top_n, metric)
    plt.figure(figsize=(12, 6))
    sns.barplot(data=top_teams, x='team', y=metric)
    plt.title(f'Top {top_n} Teams by {metric}')
    plt.xticks(rotation=45)
    plt.show()


def plot_correlation_heatmap(df: pd.DataFrame, features: Optional[List[str]] = None) -> None:
    """
    Create correlation heatmap for features.
    
    Args:
        df: DataFrame with numeric features
        features: Specific features to include (uses all numeric if None)
    """
    if features:
        corr = df[features].corr()
    else:
        corr = df.select_dtypes(include=['number']).corr()
    
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr, annot=True, fmt='.2f', cmap='coolwarm', center=0)
    plt.title('Feature Correlations')
    plt.tight_layout()
    plt.show()
