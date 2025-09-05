# selenium_scrape/management/commands/start_nse_scheduler.py
"""
Django management command to start the NSE announcement scraper scheduler.
This will run the nse_ann_selenium command every 30 minutes automatically.
"""

import logging
import sys
from datetime import datetime

from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.conf import settings

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from django_apscheduler.jobstores import DjangoJobStore
from django_apscheduler.models import DjangoJobExecution
from django_apscheduler import util

logger = logging.getLogger(__name__)


def scrape_nse_announcements():
    """
    Job function to scrape NSE announcements.
    This function will be called by the scheduler every 30 minutes.
    """
    try:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting NSE announcement scraping...")
        
        # Call the existing NSE scraping command with default parameters
        # You can modify these parameters as needed
        call_command(
            'nse_ann_selenium',
            max_rows=100,        # Scrape latest 100 rows
            headless=True,       # Run in headless mode (no browser window)
            pause=1.2,           # Pause between scrolls
            stall=4,             # Stall tolerance
            xbrl_parse=True,     # Parse XBRL data
            upload_pdfs=True,    # Upload PDFs to R2
            verbosity=1
        )
        
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] NSE announcement scraping completed successfully!")
        return "Success"
        
    except Exception as e:
        logger.error(f"Error during NSE scraping: {str(e)}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error during scraping: {str(e)}")
        raise


# The decorator is for capturing exceptions and reporting them to the database
@util.close_old_connections
def delete_old_job_executions(max_age=604_800):
    """
    Delete job executions older than `max_age` seconds.
    This helps prevent the database from filling up with old job execution records.
    Default is 7 days (604,800 seconds).
    """
    DjangoJobExecution.objects.delete_old_job_executions(max_age)


class Command(BaseCommand):
    help = "Starts the NSE announcement scraper scheduler to run every 30 minutes"

    def add_arguments(self, parser):
        parser.add_argument(
            '--interval',
            type=int,
            default=30,
            help='Interval in minutes between scraping runs (default: 30)'
        )
        parser.add_argument(
            '--max-rows',
            type=int,
            default=100,
            help='Maximum number of rows to scrape per run (default: 100)'
        )
        parser.add_argument(
            '--run-immediately',
            action='store_true',
            help='Run the scraper immediately when starting the scheduler'
        )
        parser.add_argument(
            '--no-upload',
            action='store_true',
            help='Disable PDF uploads to R2'
        )

    def handle(self, *args, **options):
        interval_minutes = options['interval']
        max_rows = options['max_rows']
        run_immediately = options['run_immediately']
        upload_pdfs = not options['no_upload']
        
        # Create scheduler
        scheduler = BackgroundScheduler(timezone=settings.TIME_ZONE if hasattr(settings, 'TIME_ZONE') else 'UTC')
        scheduler.add_jobstore(DjangoJobStore(), "default")
        
        # Modified scrape function to use command options
        def scrape_with_options():
            try:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting NSE announcement scraping...")
                call_command(
                    'nse_ann_selenium',
                    max_rows=max_rows,
                    headless=True,
                    pause=1.2,
                    stall=4,
                    xbrl_parse=True,
                    upload_pdfs=upload_pdfs,
                    verbosity=1
                )
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] NSE announcement scraping completed!")
            except Exception as e:
                logger.error(f"Error during NSE scraping: {str(e)}")
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error: {str(e)}")
                raise
        
        # Register the job
        scheduler.add_job(
            scrape_with_options,
            trigger=IntervalTrigger(minutes=interval_minutes),
            id="nse_announcement_scraper",
            max_instances=1,
            replace_existing=True,
            coalesce=True,  # If multiple jobs are pending, only run once
        )
        
        # Add job to delete old executions daily
        scheduler.add_job(
            delete_old_job_executions,
            trigger=IntervalTrigger(days=1),
            id="delete_old_job_executions",
            max_instances=1,
            replace_existing=True,
        )
        
        self.stdout.write(
            self.style.SUCCESS(
                f"NSE Announcement Scheduler Configuration:\n"
                f"  - Interval: Every {interval_minutes} minutes\n"
                f"  - Max rows per run: {max_rows}\n"
                f"  - PDF upload: {'Enabled' if upload_pdfs else 'Disabled'}\n"
                f"  - Timezone: {scheduler.timezone}\n"
            )
        )
        
        # Register event listeners for logging
        def job_executed_listener(event):
            if event.exception:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Job crashed: {event.exception}")
            else:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Job executed successfully")
        
        def job_missed_listener(event):
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Job missed!")
        
        scheduler.add_listener(job_executed_listener, mask=1 << 3 | 1 << 4)  # EVENT_JOB_EXECUTED | EVENT_JOB_ERROR
        scheduler.add_listener(job_missed_listener, mask=1 << 6)  # EVENT_JOB_MISSED
        
        try:
            self.stdout.write(self.style.NOTICE("Starting scheduler..."))
            scheduler.start()
            
            # Run immediately if requested
            if run_immediately:
                self.stdout.write(self.style.NOTICE("Running initial scraping..."))
                scrape_with_options()
            
            self.stdout.write(
                self.style.SUCCESS(
                    f"\nScheduler started successfully! NSE announcements will be scraped every {interval_minutes} minutes.\n"
                    "Press Ctrl+C to stop the scheduler.\n"
                    "Waiting for scheduled jobs...\n"
                )
            )
            
            # Keep the main thread alive
            while True:
                import time
                time.sleep(2)
                
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("\nStopping scheduler..."))
            scheduler.shutdown()
            self.stdout.write(self.style.SUCCESS("Scheduler stopped."))
            sys.exit(0)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Scheduler error: {str(e)}"))
            scheduler.shutdown()
            sys.exit(1)