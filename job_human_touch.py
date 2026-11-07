#!/usr/bin/env python3
"""Safari WashOps Human Touch worker (Franklin only).

Dry run by default. --live commits evaluations; --live --send-sms also
sends eligible queued SMS via Twilio. No SMS is sent without --send-sms.

Dependencies: pip install psycopg2-binary tzdata
Environment: DATABASE_URL (or PGHOST/PGDATABASE/PGUSER/PGPASSWORD),
TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER (for SMS).
"""
import argparse
import base64
import datetime as dt
import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import RealDictCursor

LOG = logging.getLogger('human_touch')
TZ = ZoneInfo(os.getenv('HUMAN_TOUCH_TIMEZONE', 'America/Chicago'))
RECORD_TZ = ZoneInfo(os.getenv('HUMAN_TOUCH_RECORD_TIMEZONE', 'UTC'))
LOCATION = 'FRA'  # Deliberately restricted until other locations are commissioned.
MAX_SMS_AGE_SECONDS = int(os.getenv('HUMAN_TOUCH_MAX_SMS_AGE_SECONDS', '300'))
MIN_RECORD_AGE_SECONDS = int(os.getenv('HUMAN_TOUCH_MIN_RECORD_AGE_SECONDS', '30'))


def connect():
    url = os.getenv('DATABASE_URL')
    if url:
        return psycopg2.connect(url, connect_timeout=10, application_name='job_human_touch')
    opts = {key: os.getenv(env) for key, env in (
        ('host', 'PGHOST'), ('port', 'PGPORT'), ('dbname', 'PGDATABASE'),
        ('user', 'PGUSER'), ('password', 'PGPASSWORD'), ('sslmode', 'PGSSLMODE')) if os.getenv(env)}
    if not opts:
        raise RuntimeError('Configure DATABASE_URL or PostgreSQL PG* environment variables')
    return psycopg2.connect(connect_timeout=10, application_name='job_human_touch', **opts)


def normalized_plate(value):
    return re.sub(r'[^A-Z0-9]', '', (value or '').upper())


def event_time(row):
    # Source timestamps appear to be UTC; override HUMAN_TOUCH_RECORD_TIMEZONE if needed.
    return dt.datetime.combine(row['created_on'], row['created_at'] or dt.time.min, tzinfo=RECORD_TZ)


def age_seconds(when):
    return (dt.datetime.now(dt.timezone.utc) - when.astimezone(dt.timezone.utc)).total_seconds()


def recent_enough(when):
    age = age_seconds(when)
    return 0 <= age <= MAX_SMS_AGE_SECONDS


def image_url(value):
    value = (value or '').strip()
    if value.startswith('https://'):
        return value
    base = os.getenv('WASHIFY_IMAGE_BASE_URL', '').rstrip('/')
    if base.startswith('https://') and value and not value.startswith(('http:', 'https:')):
        return base + '/' + urllib.parse.quote(value.lstrip('/'), safe='/')
    return ''


def prior_visit(cur, row):
    """Check MMC lp against historical lp, then Washify against historical lp_wshfy."""
    for column, plate in (('lp', normalized_plate(row.get('lp'))),
                          ('lp_wshfy', normalized_plate(row.get('lp_wshfy')))):
        if not plate:
            continue
        # column is a hardcoded whitelist, never user input.
        cur.execute(f'''
            SELECT created_on, created_at FROM vehicle
            WHERE tenant_id=%s AND location_id=%s
              AND regexp_replace(upper(coalesce({column},'')), '[^A-Z0-9]', '', 'g')=%s
              AND (created_on, coalesce(created_at, time '00:00')) < (%s, %s)
            ORDER BY created_on DESC, created_at DESC NULLS LAST LIMIT 1
        ''', (row['tenant_id'], row['location_id'], plate,
              row['created_on'], row['created_at'] or dt.time.min))
        found = cur.fetchone()
        if found:
            return dt.datetime.combine(found['created_on'], found['created_at'] or dt.time.min,
                                       tzinfo=RECORD_TZ), column
    return None, None


def matching_crew(cur, tenant_id, location_id, when, phone=None):
    """Return active, SMS-enabled crew on schedule at a particular instant."""
    local = when.astimezone(TZ)
    dow = (local.weekday() + 1) % 7  # Sunday=0
    prior_dow = (dow - 1) % 7
    clock = local.time().replace(tzinfo=None)
    cur.execute('''
        SELECT DISTINCT ON (phone_number) id, crew_name, phone_number
        FROM crew_schedule
        WHERE tenant_id=%s AND location_id=%s
          AND active IS TRUE AND sms_enabled IS TRUE
          AND (%s IS NULL OR phone_number=%s)
          AND (
              (day_of_week=%s AND start_time < end_time AND %s >= start_time AND %s < end_time)
              OR (day_of_week=%s AND start_time > end_time AND %s >= start_time)
              OR (day_of_week=%s AND start_time > end_time AND %s < end_time)
          )
        ORDER BY phone_number, id
    ''', (tenant_id, location_id, phone, phone, dow, clock, clock,
          dow, clock, prior_dow, clock))
    return cur.fetchall()


def message(event):
    details = event['vehicle_details'] or 'Vehicle details unavailable'
    last = ('First recorded visit' if event['attention_reason'] == 'NEW'
            else f"Last visit: {event['days_inactive']} days ago")
    result = (f"WashOps HUMAN TOUCH | {event['location'].strip()}\n"
              f"{event['license_plate'] or 'Plate unavailable'} | {details}\n"
              f"Wash: {event['wash_type'] or 'Unknown'}\n{last}\n"
              "Please greet customer and ask about today's experience.")
    if event['image_url']:
        result += '\nCar photo: ' + event['image_url']
    return result


def process_one(conn, live):
    """Evaluate and atomically commit event, eligible recipients, and processed flag."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('''
            SELECT v.* FROM vehicle v
            WHERE v.attention_processed IS FALSE AND v.location=%s
              AND EXISTS (SELECT 1 FROM attention_settings s
                          WHERE s.tenant_id=v.tenant_id AND s.location_id=v.location_id)
            ORDER BY v.created_on DESC, v.created_at DESC NULLS LAST, v.bill DESC
            LIMIT 1 FOR UPDATE OF v SKIP LOCKED
        ''', (LOCATION,))
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            return False
        key = (row['tenant_id'], row['location_id'], row['bill'], row['created_on'])
        when = event_time(row)
        age = age_seconds(when)
        # Allow MMC/Washify enrichment to finish before evaluating a new transaction.
        # Old records are evaluated for audit but can never queue SMS.
        if -MAX_SMS_AGE_SECONDS <= age < MIN_RECORD_AGE_SECONDS:
            LOG.info('Waiting for enrichment bill=%s age_seconds=%.0f', row['bill'], age)
            conn.rollback()
            return False
        cur.execute('''SELECT * FROM attention_settings
                       WHERE tenant_id=%s AND location_id=%s''', key[:2])
        settings = cur.fetchone()
        if not settings:
            LOG.warning('Missing settings for %s; leaving pending', key)
            conn.rollback()
            return False
        previous, matched_by = prior_visit(cur, row)
        valid_plate = bool(normalized_plate(row.get('lp')) or normalized_plate(row.get('lp_wshfy')))
        days = (when.date() - previous.date()).days if previous else None
        reason = ('NEW' if valid_plate and previous is None and settings['include_new_customers']
                  else 'INACTIVE' if previous is not None and days >= settings['inactive_days']
                  else None)
        LOG.info('vehicle=%s lp=%s lp_wshfy=%s matched_by=%s reason=%s days=%s sms_enabled=%s age_seconds=%.0f',
                 key, row.get('lp'), row.get('lp_wshfy'), matched_by, reason, days,
                 settings['sms_enabled'], age)
        if reason:
            details = ' '.join(str(row.get(k) or '').strip() for k in ('color', 'make', 'model')).strip()
            cur.execute('''
                INSERT INTO attention_events
                (tenant_id,location_id,location,bill,created_on,license_plate,vehicle_details,
                 wash_type,image_url,transaction_time,previous_visit,days_inactive,
                 threshold_used,attention_reason,matched_by)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (tenant_id,location_id,bill,created_on) DO NOTHING
                RETURNING id
            ''', (row['tenant_id'], row['location_id'], LOCATION, row['bill'], row['created_on'],
                  row.get('lp') or row.get('lp_wshfy'), details, row.get('wash_type'),
                  image_url(row.get('image')), when, previous, days,
                  settings['inactive_days'], reason, matched_by))
            inserted = cur.fetchone()
            queued = 0
            # Only freshly inserted events can queue recipients. Old events never requeue.
            if inserted and settings['sms_enabled'] and recent_enough(when):
                crew = matching_crew(cur, row['tenant_id'], row['location_id'], when)
                for person in crew:
                    cur.execute('''
                        INSERT INTO attention_recipients
                        (attention_event_id,crew_schedule_id,crew_name,phone_number)
                        VALUES (%s,%s,%s,%s)
                        ON CONFLICT (attention_event_id,phone_number) DO NOTHING
                    ''', (inserted['id'], person['id'], person['crew_name'], person['phone_number']))
                    queued += cur.rowcount
            LOG.info('qualifying event bill=%s event_created=%s sms_queued=%d',
                     row['bill'], bool(inserted), queued)
        cur.execute('''UPDATE vehicle SET attention_processed=TRUE
                       WHERE tenant_id=%s AND location_id=%s AND bill=%s AND created_on=%s''', key)
        if live:
            conn.commit()
        else:
            conn.rollback()
        return True


def twilio_send(to, body):
    sid = os.environ['TWILIO_ACCOUNT_SID']
    token = os.environ['TWILIO_AUTH_TOKEN']
    sender = os.environ['TWILIO_FROM_NUMBER']
    payload = urllib.parse.urlencode({'To': to, 'From': sender, 'Body': body}).encode()
    req = urllib.request.Request(
        f'https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json',
        data=payload, method='POST',
        headers={'Authorization': 'Basic ' + base64.b64encode(f'{sid}:{token}'.encode()).decode(),
                 'Content-Type': 'application/x-www-form-urlencoded'})
    with urllib.request.urlopen(req, timeout=12) as response:
        return json.loads(response.read().decode())


def send_pending(conn, limit):
    if not all(os.getenv(name) for name in
               ('TWILIO_ACCOUNT_SID', 'TWILIO_AUTH_TOKEN', 'TWILIO_FROM_NUMBER')):
        raise RuntimeError('Twilio environment variables missing; SMS not attempted')
    for _ in range(limit):
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute('''
                SELECT e.*, r.id AS recipient_id, r.phone_number
                FROM attention_recipients r
                JOIN attention_events e ON e.id=r.attention_event_id
                WHERE r.sms_status='PENDING' AND e.location=%s
                ORDER BY r.id LIMIT 1 FOR UPDATE OF r SKIP LOCKED
            ''', (LOCATION,))
            item = cur.fetchone()
            if not item:
                conn.rollback()
                break
            reason_to_skip = None
            if item['location'].strip() != LOCATION:
                reason_to_skip = 'Location not enabled for this worker'
            elif item['transaction_time'] is None or not recent_enough(item['transaction_time']):
                reason_to_skip = 'Transaction outside maximum SMS age'
            else:
                cur.execute('''SELECT sms_enabled FROM attention_settings
                               WHERE tenant_id=%s AND location_id=%s''',
                            (item['tenant_id'], item['location_id']))
                settings = cur.fetchone()
                if not settings or not settings['sms_enabled']:
                    reason_to_skip = 'SMS disabled in attention_settings'
                elif not matching_crew(cur, item['tenant_id'], item['location_id'],
                                       dt.datetime.now(dt.timezone.utc), item['phone_number']):
                    reason_to_skip = 'Recipient no longer on active SMS-enabled shift'
            if reason_to_skip:
                cur.execute('''UPDATE attention_recipients
                               SET sms_status='SKIPPED', error_message=%s WHERE id=%s''',
                            (reason_to_skip, item['recipient_id']))
                conn.commit()
                LOG.info('Skipped SMS recipient_id=%s: %s', item['recipient_id'], reason_to_skip)
                continue
            # Claim before the HTTP request to avoid automatic duplicate sends.
            # SENDING requires manual reconciliation after an interrupted request.
            cur.execute("UPDATE attention_recipients SET sms_status='SENDING' WHERE id=%s",
                        (item['recipient_id'],))
            conn.commit()
        try:
            response = twilio_send(item['phone_number'], message(item))
            with conn.cursor() as cur:
                cur.execute('''UPDATE attention_recipients
                               SET sms_status='SENT',sms_provider_id=%s,
                                   sms_sent_at=NOW(),error_message=NULL
                               WHERE id=%s''', (response.get('sid'), item['recipient_id']))
            conn.commit()
            LOG.info('Twilio accepted recipient_id=%s sid=%s',
                     item['recipient_id'], response.get('sid'))
        except Exception as exc:
            conn.rollback()
            # A timeout can mean Twilio accepted the request. Never auto-retry FAILED.
            with conn.cursor() as cur:
                cur.execute('''UPDATE attention_recipients
                               SET sms_status='FAILED',error_message=%s WHERE id=%s''',
                            (str(exc)[:2000], item['recipient_id']))
            conn.commit()
            LOG.exception('Twilio send uncertain/failed recipient_id=%s; manual review needed',
                          item['recipient_id'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--live', action='store_true', help='Commit evaluation and queue results')
    parser.add_argument('--send-sms', action='store_true', help='Send via Twilio; requires --live')
    parser.add_argument('--limit', type=int, default=25)
    parser.add_argument('--loop', action='store_true')
    parser.add_argument('--poll-seconds', type=float, default=5)
    args = parser.parse_args()
    if args.send_sms and not args.live:
        parser.error('--send-sms requires --live')
    if args.limit < 1 or args.poll_seconds <= 0:
        parser.error('--limit and --poll-seconds must be positive')
    if MAX_SMS_AGE_SECONDS <= 0 or MIN_RECORD_AGE_SECONDS < 0:
        parser.error('Invalid HUMAN_TOUCH_*_AGE_SECONDS settings')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    LOG.info('Human Touch started location=%s live=%s send_sms=%s max_age=%ss',
             LOCATION, args.live, args.send_sms, MAX_SMS_AGE_SECONDS)
    with connect() as conn:
        while True:
            for _ in range(args.limit):
                try:
                    if not process_one(conn, args.live):
                        break
                except Exception:
                    conn.rollback()
                    LOG.exception('Evaluation failed; transaction left unprocessed')
                    raise
                if not args.live:
                    break  # A rollback would select the same row again.
            if args.send_sms:
                send_pending(conn, args.limit)
            if not args.loop:
                break
            time.sleep(args.poll_seconds)


if __name__ == '__main__':
    main()
