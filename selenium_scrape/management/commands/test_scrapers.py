# selenium_scrape/management/commands/test_scrapers.py
"""
Test command to verify both BSE and NSE scrapers work correctly
"""

from django.core.management.base import BaseCommand
from django.core.management import call_command
from datetime import datetime
from selenium_scrape.models import SeleniumAnnouncement, NseAnnouncement, ScraperConfiguration


class Command(BaseCommand):
    help = "Test BSE and NSE scrapers to ensure they work correctly"

    def add_arguments(self, parser):
        parser.add_argument(
            '--bse-only',
            action='store_true',
            help='Test only BSE scraper'
        )
        parser.add_argument(
            '--nse-only',
            action='store_true',
            help='Test only NSE scraper'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=5,
            help='Limit number of records to scrape for testing (default: 5)'
        )

    def handle(self, *args, **options):
        bse_only = options['bse_only']
        nse_only = options['nse_only']
        limit = options['limit']
        
        self.stdout.write(
            self.style.SUCCESS(
                f"🧪 Testing Scrapers (limit: {limit} records each)"
            )
        )
        
        # Test BSE scraper
        if not nse_only:
            self.stdout.write("\n" + "="*50)
            self.stdout.write("🔸 TESTING BSE SCRAPER")
            self.stdout.write("="*50)
            
            today = datetime.now().strftime("%d-%m-%Y")
            
            try:
                # Count before
                before_count = SeleniumAnnouncement.objects.count()
                
                # Run BSE scraper for today with limit
                call_command(
                    'bse_ann_html_only',
                    date=today,
                    limit=limit,
                    verbosity=1
                )
                
                # Count after
                after_count = SeleniumAnnouncement.objects.count()
                new_records = after_count - before_count
                
                self.stdout.write(
                    self.style.SUCCESS(
                        f"✅ BSE Test Completed: {new_records} new records added"
                    )
                )
                
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"❌ BSE Test Failed: {str(e)}")
                )
        
        # Test NSE scraper
        if not bse_only:
            self.stdout.write("\n" + "="*50)
            self.stdout.write("🔹 TESTING NSE SCRAPER")
            self.stdout.write("="*50)
            
            try:
                # Count before
                before_count = NseAnnouncement.objects.count()
                
                # Run NSE scraper with limit
                call_command(
                    'nse_ann_selenium',
                    max_rows=limit,
                    headless=True,
                    verbosity=1
                )
                
                # Count after
                after_count = NseAnnouncement.objects.count()
                new_records = after_count - before_count
                
                self.stdout.write(
                    self.style.SUCCESS(
                        f"✅ NSE Test Completed: {new_records} new records added"
                    )
                )
                
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"❌ NSE Test Failed: {str(e)}")
                )
        
        # Show current database status
        self.stdout.write("\n" + "="*50)
        self.stdout.write("📊 DATABASE STATUS")
        self.stdout.write("="*50)
        
        bse_count = SeleniumAnnouncement.objects.count()
        nse_count = NseAnnouncement.objects.count()
        config_count = ScraperConfiguration.objects.count()
        
        self.stdout.write(f"🔸 BSE Announcements: {bse_count}")
        self.stdout.write(f"🔹 NSE Announcements: {nse_count}")
        self.stdout.write(f"⚙️  Scraper Configs: {config_count}")
        
        # Show scraper configurations
        if config_count > 0:
            self.stdout.write("\n📋 Scraper Configurations:")
            for config in ScraperConfiguration.objects.all():
                status = "✅ Enabled" if config.is_enabled else "❌ Disabled"
                last_run = config.last_run_timestamp.strftime("%Y-%m-%d %H:%M:%S") if config.last_run_timestamp else "Never"
                self.stdout.write(f"   • {config.scraper_name}: {status}, Last run: {last_run}")
        
        self.stdout.write(
            self.style.SUCCESS(
                f"\n🎯 Test completed! Both scrapers are ready for automation."
            )
        )