#!/usr/bin/env python3
"""
Quick test of stealth scraper on player rankings
Tests both Undetected Chrome and fallback Selenium with stealth features
"""

import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("=" * 70)
    logger.info("STEALTH SPOTRAC SCRAPER - PLAYER RANKINGS TEST")
    logger.info("=" * 70)
    
    # Import here to catch errors
    try:
        from spotrac_scraper_stealth import StealthSpotracScraper
    except ImportError as e:
        logger.error(f"Import failed: {e}")
        return 1
    
    try:
        logger.info("\n🚀 Initializing scraper (headless=True)...")
        scraper = StealthSpotracScraper(headless=True, use_undetected=True)
        scraper._initialize_driver()
        
        logger.info("✅ Driver initialized successfully")
        logger.info(f"   User-Agent: {scraper._get_random_user_agent()[:50]}...")
        logger.info(f"   Request delay simulation: enabled")
        logger.info(f"   Stealth JS injection: enabled")
        
        logger.info("\n🎯 Attempting to scrape 2024 player rankings...")
        logger.info("   URL: https://www.spotrac.com/nfl/rankings/player/_/year/2024/sort/cap_total")
        logger.info("   Timeout: 30 seconds per attempt × 3 retries")
        logger.info("   Anti-detection: undetected-chromedriver + Selenium stealth")
        
        df = scraper.scrape_player_rankings(2024, retries=3)
        
        if df is not None:
            logger.info(f"\n✅ SUCCESS! Extracted {len(df)} records")
            logger.info(f"\nDataFrame shape: {df.shape}")
            logger.info(f"Columns ({len(df.columns)}): {df.columns.tolist()}")
            logger.info(f"\nFirst 5 rows:\n{df.head()}")
            return 0
        else:
            logger.error("\n❌ FAILED: Could not scrape player data")
            logger.error("   This means Spotrac's anti-bot is still blocking us")
            logger.info("\n💡 Next steps:")
            logger.info("   1. Try manual Spotrac data export")
            logger.info("   2. Use alternative data source (Pro Football Reference)")
            logger.info("   3. Accept team-level aggregates as sufficient")
            return 1
            
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"\n❌ Error: {e}", exc_info=True)
        return 1
    finally:
        if 'scraper' in locals() and scraper.driver:
            scraper.driver.quit()

if __name__ == '__main__':
    sys.exit(main())
