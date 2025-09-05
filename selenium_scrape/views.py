import subprocess
import tempfile
import sys
import os
from pathlib import Path
import pandas as pd
from rest_framework import viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
from django.db import transaction
from django.utils import timezone
from datetime import timedelta
from .models import SeleniumAnnouncement, BseStockQuote
from .serializers import SeleniumAnnouncementSerializer, BseStockQuoteSerializer
from django.shortcuts import render
import logging
import time
from contextlib import contextmanager

# Set up logging
logger = logging.getLogger(__name__)

def announcements_page(request):
    """Render the stock quotes dashboard."""
    return render(request, "basic_industry.html")

@contextmanager
def managed_temp_file(suffix=".log", mode="w", encoding="utf-8"):
    """Context manager for properly handling temporary files."""
    temp_file = None
    log_path = None
    try:
        temp_file = tempfile.NamedTemporaryFile(
            delete=False, 
            suffix=suffix, 
            mode=mode, 
            encoding=encoding
        )
        log_path = temp_file.name
        yield temp_file, log_path
    finally:
        # Ensure file is closed before deletion
        if temp_file:
            try:
                temp_file.close()
            except:
                pass
        
        # Clean up file with retry mechanism
        if log_path and os.path.exists(log_path):
            for attempt in range(3):
                try:
                    os.unlink(log_path)
                    break
                except (OSError, PermissionError) as e:
                    if attempt < 2:
                        time.sleep(0.1)
                    else:
                        logger.warning(f"Failed to delete log file {log_path}: {e}")

class SeleniumAnnouncementViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet for managing Selenium announcements."""
    queryset = SeleniumAnnouncement.objects.all().order_by("-created_at")
    serializer_class = SeleniumAnnouncementSerializer

    @action(detail=False, methods=["post"])
    def run_scraper(self, request):
        """Trigger Selenium scraper for announcements."""
        project_root = Path(settings.BASE_DIR)
        cmd = [sys.executable, "manage.py", "scrape_bse_ann_html_only", "--max-pages", "5"]

        with managed_temp_file() as (log_file, log_path):
            rc = subprocess.call(cmd, stdout=log_file, stderr=log_file, cwd=project_root)
            
            # Read log after subprocess completes
            try:
                with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                    tail = f.read()[-2000:]
            except Exception:
                tail = "Could not read log output"

        return Response({
            "status": "success" if rc == 0 else "error",
            "return_code": rc,
            "log_tail": tail,
        }, status=status.HTTP_200_OK if rc == 0 else status.HTTP_500_INTERNAL_SERVER_ERROR)

def run_single_scripcode(scripcode, project_root, delay=3.0, timeout=300):
    """
    Simple single scripcode processing without batching complexity.
    """
    process = None
    try:
        with managed_temp_file(suffix=f"_bse_quote_{scripcode}.log") as (log_file, log_path):
            cmd = [
                sys.executable,
                "manage.py",
                "bse_quote_selenium",
                "--scripcode",
                scripcode,
                "--sleep",
                "3.0",
                "--delay",
                str(delay),
            ]

            # Set environment for UTF-8 encoding
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8'
            env['PYTHONUNBUFFERED'] = '1'

            logger.info(f"Starting process for scripcode {scripcode}")
            
            process = subprocess.Popen(
                cmd, 
                stdout=log_file, 
                stderr=subprocess.STDOUT,
                cwd=project_root,
                env=env
            )
            
            # Wait for completion
            rc = process.wait(timeout=timeout)
            
            # Ensure log file is flushed
            log_file.flush()
            os.fsync(log_file.fileno())
            
            # Read log content
            try:
                with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                    full_log = f.read()
                    log_tail = full_log[-2000:] if len(full_log) > 2000 else full_log
            except Exception as e:
                log_tail = f"Could not read log output: {str(e)}"

            # Check database for results
            try:
                time.sleep(0.5)  # Brief wait for database transaction
                record = BseStockQuote.objects.filter(scripcode=scripcode).order_by('-scraped_at').first()
                
                if not record:
                    status_str = "no_record"
                    message = f"No database record created for {scripcode}"
                    quote_exists = False
                elif record.error_message:
                    status_str = "error"
                    message = f"Error: {record.error_message}"
                    quote_exists = True
                elif record.security_name and record.basic_industry:
                    status_str = "success"
                    message = f"Successfully scraped {scripcode}: {record.security_name}"
                    quote_exists = True
                elif record.security_name:
                    status_str = "partial"
                    message = f"Partial success: {record.security_name} (missing Basic Industry)"
                    quote_exists = True
                else:
                    status_str = "incomplete"
                    message = f"Incomplete data for {scripcode}"
                    quote_exists = True
                    
            except Exception as db_e:
                logger.error(f"Database check failed for {scripcode}: {str(db_e)}")
                status_str = "db_error"
                message = f"Database verification failed: {str(db_e)}"
                quote_exists = False

            result = {
                "scripcode": scripcode,
                "status": status_str,
                "return_code": rc,
                "log_tail": log_tail,
                "message": message,
                "quote_exists": quote_exists
            }
            
            logger.info(f"Completed {scripcode}: {status_str} (RC: {rc})")
            return result
            
    except subprocess.TimeoutExpired:
        logger.warning(f"Process timeout for {scripcode} after {timeout} seconds")
        
        if process:
            try:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            except Exception as e:
                logger.error(f"Error terminating process for {scripcode}: {str(e)}")
        
        return {
            "scripcode": scripcode,
            "status": "timeout",
            "message": f"Process timeout after {timeout} seconds",
            "log_tail": "Process timed out",
            "quote_exists": False
        }
        
    except Exception as e:
        if process:
            try:
                process.terminate()
                process.wait(timeout=5)
            except:
                try:
                    process.kill()
                except:
                    pass
        
        logger.error(f"Exception for {scripcode}: {str(e)}")
        return {
            "scripcode": scripcode,
            "status": "exception",
            "message": f"Exception: {str(e)}",
            "log_tail": f"Process exception: {str(e)}",
            "quote_exists": False
        }

class BseStockQuoteViewSet(viewsets.ReadOnlyModelViewSet):
    """Simplified ViewSet for BSE stock quotes without batching complexity."""
    queryset = BseStockQuote.objects.all().order_by("-scraped_at")
    serializer_class = BseStockQuoteSerializer

    @action(detail=False, methods=["post"])
    def scrape_quotes_from_excel(self, request):
        """
        Simplified Excel processing - processes each scripcode individually.
        """
        logger.info("Processing Excel file for BSE scrip codes")

        if 'file' not in request.FILES:
            return Response({
                "status": "error",
                "message": "No file provided. Please upload an Excel file."
            }, status=status.HTTP_400_BAD_REQUEST)

        excel_file = request.FILES['file']
        project_root = Path(settings.BASE_DIR)

        try:
            logger.info(f"Reading Excel file: {excel_file.name}")
            df = pd.read_excel(excel_file, engine='openpyxl')
            
            # Find scripcode column
            possible_columns = [
                'CD_BSE Code', 'CD BSE Code', 'scripcode', 'BSE Code', 
                'bse_code', 'code', 'BSE_Code', 'Scripcode', 'SCRIPCODE'
            ]
            scripcode_column = None
            
            df_columns_lower = {col.lower(): col for col in df.columns}
            for col in possible_columns:
                if col.lower() in df_columns_lower:
                    scripcode_column = df_columns_lower[col.lower()]
                    break
            
            if scripcode_column is None:
                return Response({
                    "status": "error",
                    "message": f"Excel file must contain one of these columns: {', '.join(possible_columns)}. Found: {', '.join(df.columns)}"
                }, status=status.HTTP_400_BAD_REQUEST)

            # Clean and validate scripcode data
            raw_scripcodes = df[scripcode_column].dropna()
            
            if raw_scripcodes.empty:
                return Response({
                    "status": "error",
                    "message": f"No valid BSE codes found in column '{scripcode_column}'."
                }, status=status.HTTP_400_BAD_REQUEST)

            valid_scripcodes = []
            skipped_results = []
            
            for original_code in raw_scripcodes:
                try:
                    if pd.isna(original_code):
                        continue
                        
                    code_str = str(original_code).strip()
                    cleaned_code = ''.join(ch for ch in code_str.split('.')[0] if ch.isdigit())
                    
                    if cleaned_code and len(cleaned_code) >= 3 and len(cleaned_code) <= 10:
                        valid_scripcodes.append(cleaned_code)
                    else:
                        skipped_results.append({
                            "scripcode": str(original_code),
                            "status": "skipped",
                            "message": f"Invalid BSE code '{original_code}' - must be 3-10 digits."
                        })
                except Exception as e:
                    skipped_results.append({
                        "scripcode": str(original_code),
                        "status": "skipped",
                        "message": f"Error processing '{original_code}': {str(e)}"
                    })

            # Remove duplicates
            seen = set()
            unique_scripcodes = []
            for code in valid_scripcodes:
                if code not in seen:
                    seen.add(code)
                    unique_scripcodes.append(code)

            valid_scripcodes = unique_scripcodes
            logger.info(f"Found {len(valid_scripcodes)} valid unique BSE codes")

            if not valid_scripcodes:
                return Response({
                    "status": "error",
                    "message": "No valid BSE codes found after cleaning.",
                    "skipped_codes": skipped_results
                }, status=status.HTTP_400_BAD_REQUEST)

            # Skip recently scraped codes (optional)
            cutoff_time = timezone.now() - timedelta(hours=2)
            recently_scraped = set(
                BseStockQuote.objects.filter(
                    scripcode__in=valid_scripcodes,
                    scraped_at__gte=cutoff_time,
                    error_message__isnull=True,
                    security_name__isnull=False,
                    basic_industry__isnull=False
                ).values_list('scripcode', flat=True)
            )

            codes_to_process = [code for code in valid_scripcodes if code not in recently_scraped]
            
            for skipped_code in recently_scraped:
                skipped_results.append({
                    "scripcode": skipped_code,
                    "status": "skipped",
                    "message": f"Complete data exists within 2 hours for {skipped_code}"
                })

            if not codes_to_process:
                return Response({
                    "status": "completed",
                    "message": f"All {len(valid_scripcodes)} codes have recent complete data.",
                    "results": skipped_results,
                    "summary": {
                        "total": len(raw_scripcodes),
                        "successful": 0,
                        "errors": 0,
                        "skipped": len(skipped_results),
                        "column_used": scripcode_column
                    }
                }, status=status.HTTP_200_OK)

            # Process each code individually (no batching)
            all_results = list(skipped_results)
            
            logger.info(f"Processing {len(codes_to_process)} codes individually")

            for i, scripcode in enumerate(codes_to_process):
                logger.info(f"Processing {i+1}/{len(codes_to_process)}: {scripcode}")
                
                result = run_single_scripcode(scripcode, project_root, delay=3.0, timeout=300)
                all_results.append(result)
                
                logger.info(f"Result for {scripcode}: {result['status']}")
                
                # Simple delay between codes
                if i < len(codes_to_process) - 1:  # Don't delay after last code
                    time.sleep(3.0)

            # Calculate summary
            successful_count = len([r for r in all_results if r['status'] == 'success'])
            error_count = len([r for r in all_results if r['status'] in ['error', 'exception', 'timeout']])
            skipped_count = len([r for r in all_results if r['status'] == 'skipped'])
            partial_count = len([r for r in all_results if r['status'] == 'partial'])

            logger.info(f"Results: {successful_count} successful, {partial_count} partial, {error_count} errors, {skipped_count} skipped")
            
            overall_status = "completed" if error_count == 0 else "completed_with_errors"
            
            return Response({
                "status": overall_status,
                "message": f"Processed {len(codes_to_process)} codes. {successful_count} successful, {partial_count} partial, {error_count} errors, {skipped_count} skipped.",
                "results": all_results,
                "summary": {
                    "total_input": len(raw_scripcodes),
                    "valid_codes": len(valid_scripcodes),
                    "processed": len(codes_to_process),
                    "successful": successful_count,
                    "partial": partial_count,
                    "errors": error_count,
                    "skipped": skipped_count,
                    "column_used": scripcode_column
                }
            }, status=status.HTTP_200_OK)

        except pd.errors.EmptyDataError:
            return Response({
                "status": "error",
                "message": "Excel file is empty."
            }, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            logger.error(f"Failed to process Excel file: {str(e)}")
            return Response({
                "status": "error",
                "message": f"Failed to process Excel file: {str(e)}"
            }, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=["post"])
    def scrape_single_quote(self, request):
        """
        Simple single quote scraping without batching logic.
        """
        scripcode = request.data.get('scripcode', '').strip()
        
        if not scripcode:
            return Response({
                "status": "error",
                "message": "Please provide a scripcode in the request body."
            }, status=status.HTTP_400_BAD_REQUEST)

        # Clean scripcode
        clean_scripcode = ''.join(ch for ch in scripcode.split('.')[0] if ch.isdigit())
        if not clean_scripcode or len(clean_scripcode) < 3:
            return Response({
                "status": "error",
                "message": f"Invalid scripcode '{scripcode}'. Must contain at least 3 digits."
            }, status=status.HTTP_400_BAD_REQUEST)

        logger.info(f"Processing single scripcode: {clean_scripcode}")
        project_root = Path(settings.BASE_DIR)
        
        # Process the single code
        result = run_single_scripcode(clean_scripcode, project_root, delay=3.0, timeout=300)
        
        # Get database record for verification
        try:
            db_record = BseStockQuote.objects.filter(scripcode=clean_scripcode).order_by('-scraped_at').first()
        except Exception as e:
            logger.error(f"Error fetching database record: {str(e)}")
            db_record = None
        
        response_data = {
            "status": result['status'],
            "message": result['message'],
            "scripcode": clean_scripcode,
            "log_tail": result.get('log_tail', ''),
            "quote_exists": result.get('quote_exists', False),
            "return_code": result.get('return_code')
        }
        
        # Add current record info if exists
        if db_record:
            response_data["record"] = {
                "security_name": db_record.security_name,
                "basic_industry": db_record.basic_industry,
                "scraped_at": db_record.scraped_at,
                "has_error": bool(db_record.error_message),
                "error_message": db_record.error_message,
                "is_complete": bool(db_record.security_name and db_record.basic_industry)
            }
        
        return Response(response_data, status=status.HTTP_200_OK)

    @action(detail=False, methods=["get"])
    def stats(self, request):
        """Simple statistics overview."""
        total_quotes = BseStockQuote.objects.count()
        
        complete_quotes = BseStockQuote.objects.filter(
            error_message__isnull=True,
            security_name__isnull=False,
            basic_industry__isnull=False
        ).count()
        
        partial_quotes = BseStockQuote.objects.filter(
            error_message__isnull=True,
            security_name__isnull=False,
            basic_industry__isnull=True
        ).count()
        
        error_quotes = BseStockQuote.objects.filter(
            error_message__isnull=False
        ).count()
        
        recent_quotes = BseStockQuote.objects.filter(
            scraped_at__gte=timezone.now() - timedelta(hours=24)
        ).count()
        
        return Response({
            "total_quotes": total_quotes,
            "complete_quotes": complete_quotes,
            "partial_quotes": partial_quotes,
            "error_quotes": error_quotes,
            "recent_24h": recent_quotes,
            "success_rate": round((complete_quotes / total_quotes * 100), 2) if total_quotes > 0 else 0
        }, status=status.HTTP_200_OK)

    @action(detail=False, methods=["delete"])
    def clear_errors(self, request):
        """Clear error records."""
        error_count = BseStockQuote.objects.filter(error_message__isnull=False).count()
        
        if error_count == 0:
            return Response({
                "status": "info",
                "message": "No error records found to clear."
            }, status=status.HTTP_200_OK)
        
        with transaction.atomic():
            deleted_count, _ = BseStockQuote.objects.filter(error_message__isnull=False).delete()
        
        logger.info(f"Cleared {deleted_count} error records")
        
        return Response({
            "status": "success",
            "message": f"Cleared {deleted_count} error records.",
            "deleted_count": deleted_count
        }, status=status.HTTP_200_OK)

    @action(detail=False, methods=["post"])
    def test_single_code(self, request):
        """
        Test a single code with detailed logging for debugging.
        """
        scripcode = request.data.get('scripcode', '').strip()
        
        if not scripcode:
            return Response({
                "status": "error",
                "message": "Please provide a scripcode."
            }, status=status.HTTP_400_BAD_REQUEST)

        clean_scripcode = ''.join(ch for ch in scripcode.split('.')[0] if ch.isdigit())
        
        # Get existing records
        existing_records = BseStockQuote.objects.filter(scripcode=clean_scripcode).order_by('-scraped_at')[:3]
        
        logger.info(f"Testing scripcode: {clean_scripcode}")
        project_root = Path(settings.BASE_DIR)
        
        # Run the scraping
        result = run_single_scripcode(clean_scripcode, project_root, delay=3.0, timeout=300)
        
        # Get new record
        new_record = BseStockQuote.objects.filter(scripcode=clean_scripcode).order_by('-scraped_at').first()
        
        return Response({
            "test_result": result,
            "scripcode": clean_scripcode,
            "existing_records": [
                {
                    "security_name": record.security_name,
                    "basic_industry": record.basic_industry,
                    "scraped_at": record.scraped_at,
                    "error_message": record.error_message
                } for record in existing_records
            ],
            "new_record": {
                "security_name": new_record.security_name if new_record else None,
                "basic_industry": new_record.basic_industry if new_record else None,
                "scraped_at": new_record.scraped_at if new_record else None,
                "error_message": new_record.error_message if new_record else None,
                "exists": new_record is not None
            } if new_record else None
        }, status=status.HTTP_200_OK)