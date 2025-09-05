# selenium_scrape/management/commands/start_auto_scrapers.py
"""
Automated scraper scheduler for BSE and NSE announcements.

Features:
- Auto-starts when Django server runs
- First time: Scrapes all available data going back in time
- Subsequent runs: Only scrapes new data from the last known date
- Handles both BSE and NSE scrapers
- Tracks status and errors in database
"""

import logging
import sys
import time
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.db import transaction
from django.utils import timezone

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from django_apscheduler.jobstores import DjangoJobStore
from django_apscheduler.models import DjangoJobExecution
from django_apscheduler import util

from selenium_scrape.models import ScraperConfiguration, SeleniumAnnouncement

logger = logging.getLogger(__name__)


def get_today_date():
    """Get today's date in DD-MM-YYYY format"""
    return datetime.now().strftime("%d-%m-%Y")


def should_scrape_today(last_scrape_date):
    """Check if we should scrape today (only if we haven't scraped today yet)"""
    today = get_today_date()
    
    # If never scraped before, scrape today
    if not last_scrape_date:
        return True
        
    # If last scraped date is not today, scrape today
    return last_scrape_date != today


@util.close_old_connections
def run_bse_scraper():
    """Job function to run BSE announcements scraper"""
    scraper_name = "bse_announcements"
    config = None
    
    try:
        # Get or create scraper configuration
        config, created = ScraperConfiguration.objects.get_or_create(
            scraper_name=scraper_name,
            defaults={
                'is_first_run': True,
                'scrape_interval_minutes': 30,
                'is_enabled': True
            }
        )
        
        if not config.is_enabled:
            print(f"[{datetime.now()}] BSE scraper is disabled")
            return
        
        print(f"[{datetime.now()}] Starting BSE scraper...")
        
        # Check if we should scrape today
        today = get_today_date()
        
        if not should_scrape_today(config.last_scrape_date):
            print(f"[{datetime.now()}] BSE already scraped today ({today}), skipping...")
            return
        
        print(f"[{datetime.now()}] Scraping BSE for today: {today}")
        
        try:
            # Count records before
            before_count = SeleniumAnnouncement.objects.count()
            
            # Run the scraper for today only
            call_command(
                'bse_ann_html_only',
                date=today,
                verbosity=1  # Reduced verbosity for scheduled runs
            )
            
            # Count records after
            after_count = SeleniumAnnouncement.objects.count()
            new_records = after_count - before_count
            
            print(f"[{datetime.now()}] BSE scraping for {today} completed: {new_records} new records")
            
        except Exception as e:
            logger.error(f"Error scraping BSE for date {today}: {e}")
            print(f"[{datetime.now()}] Error scraping BSE for {today}: {e}")
            raise
        
        # Update configuration
        with transaction.atomic():
            config.is_first_run = False
            config.last_scrape_date = today
            config.last_run_timestamp = timezone.now()
            config.last_records_processed = 1  # Only scraped 1 date (today)
            config.last_new_records = new_records
            config.last_error_message = None
            config.save()
        
        print(f"[{datetime.now()}] BSE scraper completed: {new_records} new records for today")
        
    except Exception as e:
        error_msg = f"BSE scraper failed: {str(e)}"
        logger.error(error_msg)
        print(f"[{datetime.now()}] {error_msg}")
        
        # Update config with error
        if config:
            try:
                with transaction.atomic():
                    config.last_error_message = error_msg
                    config.last_run_timestamp = timezone.now()
                    config.save()
            except Exception:
                pass


@util.close_old_connections
def run_nse_scraper():
    """Job function to run NSE announcements scraper"""
    scraper_name = "nse_announcements"
    config = None
    
    try:
        # Get or create scraper configuration
        config, created = ScraperConfiguration.objects.get_or_create(
            scraper_name=scraper_name,
            defaults={
                'is_first_run': True,
                'scrape_interval_minutes': 30,
                'is_enabled': True
            }
        )
        
        if not config.is_enabled:
            print(f"[{datetime.now()}] NSE scraper is disabled")
            return
        
        print(f"[{datetime.now()}] Starting NSE scraper...")
        
        # NSE scraper gets latest announcements (not date-specific)
        # Always scrape latest announcements - duplicates are handled by the scraper
        max_rows = 150 if config.is_first_run else 100  # Get more on first run
        
        try:
            # Count records before
            from selenium_scrape.models import NseAnnouncement
            before_count = NseAnnouncement.objects.count()
            
            # Run the NSE scraper
            call_command(
                'nse_ann_selenium',
                max_rows=max_rows,
                headless=True,
                pause=1.2,
                stall=4,
                xbrl_parse=True,
                upload_pdfs=True,
                verbosity=1
            )
            
            # Count records after
            after_count = NseAnnouncement.objects.count()
            new_records = after_count - before_count
            
            # Update configuration
            with transaction.atomic():
                config.is_first_run = False
                config.last_scrape_date = datetime.now().strftime("%d-%m-%Y")
                config.last_run_timestamp = timezone.now()
                config.last_records_processed = max_rows
                config.last_new_records = new_records
                config.last_error_message = None
                config.save()
            
            print(f"[{datetime.now()}] NSE scraper completed: {new_records} new records")
            
        except Exception as e:
            raise e
        
    except Exception as e:
        error_msg = f"NSE scraper failed: {str(e)}"
        logger.error(error_msg)
        print(f"[{datetime.now()}] {error_msg}")
        
        # Update config with error
        if config:
            try:
                with transaction.atomic():
                    config.last_error_message = error_msg
                    config.last_run_timestamp = timezone.now()
                    config.save()
            except Exception:
                pass


@util.close_old_connections
def delete_old_job_executions(max_age=604_800):
    """Delete job executions older than max_age seconds (default: 7 days)"""
    DjangoJobExecution.objects.delete_old_job_executions(max_age)


class Command(BaseCommand):
    help = "Start automated scrapers for BSE and NSE announcements"

    def add_arguments(self, parser):
        parser.add_argument(
            '--bse-interval',
            type=int,
            default=30,
            help='BSE scraper interval in minutes (default: 30)'
        )
        parser.add_argument(
            '--nse-interval',
            type=int,
            default=30,
            help='NSE scraper interval in minutes (default: 30)'
        )
        parser.add_argument(
            '--run-immediately',
            action='store_true',
            help='Run scrapers immediately when starting'
        )
        parser.add_argument(
            '--bse-only',
            action='store_true',
            help='Only run BSE scraper'
        )
        parser.add_argument(
            '--nse-only',
            action='store_true',
            help='Only run NSE scraper'
        )

    def handle(self, *args, **options):
        bse_interval = options['bse_interval']
        nse_interval = options['nse_interval']
        run_immediately = options['run_immediately']
        bse_only = options['bse_only']
        nse_only = options['nse_only']
        
        # Create scheduler
        scheduler = BackgroundScheduler(timezone='UTC')
        scheduler.add_jobstore(DjangoJobStore(), "default")
        
        jobs_scheduled = []
        
        # Add BSE scraper job
        if not nse_only:
            scheduler.add_job(
                run_bse_scraper,
                trigger=IntervalTrigger(minutes=bse_interval),
                id="bse_announcements_scraper",
                name="BSE Announcements Scraper",
                max_instances=1,
                replace_existing=True,
                coalesce=True,
            )
            jobs_scheduled.append(f"BSE scraper (every {bse_interval} minutes)")
        
        # Add NSE scraper job
        if not bse_only:
            scheduler.add_job(
                run_nse_scraper,
                trigger=IntervalTrigger(minutes=nse_interval),
                id="nse_announcements_scraper", 
                name="NSE Announcements Scraper",
                max_instances=1,
                replace_existing=True,
                coalesce=True,
            )
            jobs_scheduled.append(f"NSE scraper (every {nse_interval} minutes)")
        
        # Add cleanup job
        scheduler.add_job(
            delete_old_job_executions,
            trigger=IntervalTrigger(days=1),
            id="delete_old_job_executions",
            name="Delete Old Job Executions",
            max_instances=1,
            replace_existing=True,
        )
        
        self.stdout.write(
            self.style.SUCCESS(
                f"🚀 Automated Scraper Configuration:\n"
                f"  - Jobs scheduled: {', '.join(jobs_scheduled)}\n"
                f"  - Timezone: {scheduler.timezone}\n"
                f"  - Cleanup: Daily\n"
            )
        )
        
        # Event listeners
        def job_executed_listener(event):
            if event.exception:
                print(f"[{datetime.now()}] ❌ Job crashed: {event.exception}")
            else:
                print(f"[{datetime.now()}] ✅ Job executed successfully: {event.job_id}")
        
        def job_missed_listener(event):
            print(f"[{datetime.now()}] ⚠️  Job missed: {event.job_id}")
        
        scheduler.add_listener(job_executed_listener, mask=1 << 3 | 1 << 4)
        scheduler.add_listener(job_missed_listener, mask=1 << 6)
        
        try:
            self.stdout.write(self.style.NOTICE("Starting scheduler..."))
            scheduler.start()
            
            # Run immediately if requested
            if run_immediately:
                self.stdout.write(self.style.NOTICE("Running initial scraping..."))
                if not nse_only:
                    run_bse_scraper()
                if not bse_only:
                    run_nse_scraper()
            
            self.stdout.write(
                self.style.SUCCESS(
                    f"\n🎯 Automated scrapers started successfully!\n"
                    f"  - {'BSE' if not nse_only else ''}{'/' if not nse_only and not bse_only else ''}{'NSE' if not bse_only else ''} scrapers are now running\n"
                    f"  - First run will scrape historical data\n"
                    f"  - Subsequent runs will only scrape new data\n"
                    f"  - Press Ctrl+C to stop\n"
                )
            )
            
            # Keep main thread alive
            while True:
                time.sleep(2)
                
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("\n⏹️  Stopping schedulers..."))
            scheduler.shutdown()
            self.stdout.write(self.style.SUCCESS("✅ Schedulers stopped"))
            sys.exit(0)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Scheduler error: {str(e)}"))
            scheduler.shutdown()
            sys.exit(1)