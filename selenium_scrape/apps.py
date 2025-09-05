from django.apps import AppConfig
import os
import threading


class SeleniumScrapeConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'selenium_scrape'
    
    def ready(self):
        """Called when Django starts - auto-start scrapers if enabled"""
        # Only start in main process, not in reloader subprocess
        if os.environ.get('RUN_MAIN') == 'true':
            self.start_scrapers_if_enabled()
    
    def start_scrapers_if_enabled(self):
        """Start automated scrapers if AUTO_START_SCRAPERS is enabled"""
        from django.conf import settings
        
        # Check if auto-start is enabled in settings
        if getattr(settings, 'AUTO_START_SCRAPERS', False):
            # Start in separate thread to avoid blocking Django startup
            def start_scrapers():
                try:
                    import time
                    time.sleep(10)  # Wait 10 seconds for Django to fully start
                    
                    from django.core.management import call_command
                    print("[DJANGO STARTUP] Auto-starting scrapers...")
                    
                    call_command(
                        'start_auto_scrapers',
                        run_immediately=True,  # Run initial scrape
                        verbosity=1
                    )
                except Exception as e:
                    print(f"[DJANGO STARTUP] Failed to auto-start scrapers: {e}")
            
            thread = threading.Thread(target=start_scrapers, daemon=True)
            thread.start()
            print("[DJANGO STARTUP] Scraper auto-start thread initiated")
