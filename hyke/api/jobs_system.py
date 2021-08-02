from dateutil.relativedelta import relativedelta
from django import db
from django.db.models import Q
from django.utils import timezone
from hyke.api.models import (
    ProgressStatus,
    StatusEngine,
)
from hyke.automation.jobs import (
    nps_calculator_onboarding,
    nps_calculator_running,
)
from hyke.email.jobs import send_transactional_email
from hyke.fms.jobs import create_dropbox_folders
from hyke.scheduled.base import next_annualreport_reminder
from hyke.scheduled.service.nps_surveys import (
    schedule_next_running_survey_sequence,
    schedule_onboarding_survey_sequence,
    send_client_onboarding_survey,
)
from structlog import get_logger

logger = get_logger(__name__)


def client_onboarding_survey(item : StatusEngine):
    try:
        send_client_onboarding_survey(email=item.email)
    except Exception as e:
        logger.exception(f"Can't process Onboarding NPS Survey for status engine id={item.id}")


def payment_error_email(item : StatusEngine):
    send_transactional_email(
        email=item.email, template="[Action required] - Please update your payment information",
    )
    print("[Action required] - Please update your payment information email is sent to " + item.email)


def running_flow(item : StatusEngine):
    ps = ProgressStatus.objects.get(email=item.email)
    ps.bookkeepingsetupstatus = "completed"
    ps.taxsetupstatus = "completed2"
    ps.save()

    StatusEngine.objects.get_or_create(
        email=item.email,
        process="Schedule Email",
        formationtype="Hyke Daily",
        processstate=1,
        outcome=StatusEngine.SCHEDULED,
        data="What's upcoming with Collective?",
        defaults={"executed": timezone.now() + relativedelta(days=1)},
    )

    StatusEngine.objects.get_or_create(
        email=item.email,
        process="Running flow",
        formationtype="Hyke System",
        processstate=2,
        defaults={"outcome": StatusEngine.SCHEDULED, "data": "---"},
    )

    schedule_onboarding_survey_sequence(email=item.email)
    schedule_next_running_survey_sequence(email=item.email)

    create_dropbox_folders(email=item.email)

    print("Dropbox folders are created for " + item.email)

    has_run_before = StatusEngine.objects.filter(
        email=item.email, process=item.process, processstate=item.processstate, outcome=1,
    ).exists()

    if has_run_before:
        print(
            "Not creating form w9 or emailing pops because dropbox folders job has already run for {}".format(
                item.email
            )
        )


def annual_report_uploaded(item : StatusEngine):
    reportdetails = item.data.split("---")
    reportname = reportdetails[1].strip()
    reportyear = reportdetails[0].strip()
    reportstate = reportdetails[2].strip() if len(reportdetails) == 3 else None

    data_filter = Q(data=f"{reportyear} --- {reportname}")
    if reportstate:
        data_filter |= Q(data=f"{reportyear} --- {reportname} --- {reportstate}")

    SEs = StatusEngine.objects.filter(email=item.email, process="Annual Report Reminder", outcome=-1).filter(
        data_filter
    )
    for se in SEs:
        se.outcome = 1
        se.executed = timezone.now()
        se.save()

    # complete this before we schedule the next reminder
    item.outcome = StatusEngine.COMPLETED
    item.executed = timezone.now()
    item.save()

    next_annualreport_reminder(item.email, reportname, reportstate)


def kickoff_questionnaire_completed(item : StatusEngine):
    progress_status = ProgressStatus.objects.filter(email__iexact=item.email).first()
    if progress_status:
        progress_status.questionnairestatus = "scheduled"
        progress_status.save()

        StatusEngine.objects.create(
            email=item.email,
            processstate=1,
            formationtype="Hyke Salesforce",
            outcome=-1,
            process="Kickoff Questionnaire Completed",
            data=item.data,
        )


def kickoff_call_scheduled(item : StatusEngine):
    progress_status = ProgressStatus.objects.get(email__iexact=item.email)
    progress_status.questionnairestatus = "scheduled"
    progress_status.save()

    StatusEngine.objects.create(
        email=item.email,
        processstate=1,
        formationtype="Hyke Salesforce",
        outcome=-1,
        process="Kickoff Call Scheduled",
        data=item.data,
    )


def kickoff_call_cancelled(item : StatusEngine):
    progress_status = ProgressStatus.objects.get(email__iexact=item.email)
    progress_status.questionnairestatus = "reschedule"
    progress_status.save()

    StatusEngine.objects.create(
        email=item.email,
        processstate=1,
        formationtype="Hyke Salesforce",
        outcome=-1,
        process="Kickoff Call Cancelled",
    )


def transition_plan_submitted(item : StatusEngine):
    progress_status = ProgressStatus.objects.get(email__iexact=item.email)
    progress_status.questionnairestatus = "submitted"
    progress_status.save()

    StatusEngine.objects.create(
        email=item.email,
        process="Transition Plan Submitted",
        formationtype="Hyke Salesforce",
        processstate=1,
        outcome=StatusEngine.SCHEDULED,
        data="---",
    )

    StatusEngine.objects.get_or_create(
        email=item.email,
        process="Schedule Email",
        formationtype="Hyke Daily",
        processstate=1,
        outcome=StatusEngine.SCHEDULED,
        data="Welcome to the Collective community!",
        defaults={"executed": timezone.now() + relativedelta(days=1)},
    )


def bk_training_call_scheduled(item : StatusEngine):
    StatusEngine.objects.create(
        email=item.email,
        processstate=1,
        formationtype="Hyke Salesforce",
        outcome=-1,
        process="BK Training Call Scheduled",
        data=item.data,
    )


def bk_training_call_cancelled(item : StatusEngine):
    progress_status = ProgressStatus.objects.get(email__iexact=item.email)
    progress_status.bookkeepingsetupstatus = "reschedule"
    progress_status.save()

    status_engine = StatusEngine(
        email=item.email,
        process="Followup - BK Training",
        formationtype="Hyke Daily",
        processstate=1,
        outcome=-1,
        data="---",
        executed=timezone.now() + relativedelta(days=2),
    )
    status_engine.save()

    StatusEngine.objects.create(
        email=item.email,
        processstate=1,
        formationtype="Hyke Salesforce",
        outcome=-1,
        process="BK Training Call Cancelled",
    )


def calculate_nps_running(item : StatusEngine):
    nps_calculator_running()

    print("Running NPS is calculated for " + item.data)


def calculate_nps_onboarding(item : StatusEngine):
    nps_calculator_onboarding()

    print("Onboarding NPS is calculated for " + item.data)


FUNCTION_MAP_STATE_1 = {
    "Client Onboarding Survey": client_onboarding_survey,
    "Payment error email": payment_error_email,
    "Running flow": running_flow,
    "Kickoff Questionnaire Completed": kickoff_questionnaire_completed,
    "Kickoff Call Scheduled": kickoff_call_scheduled,
    "Kickoff Call Cancelled": kickoff_call_cancelled,
    "Transition Plan Submitted": transition_plan_submitted,
    "BK Training Call Scheduled": bk_training_call_scheduled,
    "BK Training Call Cancelled": bk_training_call_cancelled,
}


FUNCTION_MAP_STATELESS = {
    "Annual Report Uploaded": annual_report_uploaded,
    "Calculate NPS Running": calculate_nps_running,
    "Calculate NPS Onboarding": calculate_nps_onboarding,
}


def scheduled_system():
    print("Scheduled task is started for Hyke System...")

    items_state_1 = list(StatusEngine.objects.filter(
        outcome=StatusEngine.SCHEDULED,
        formationtype__startswith="Hyke System",
        processstate=1,
        process__in=FUNCTION_MAP_STATE_1.keys()
    ))

    items_stateless = list(StatusEngine.objects.filter(
        outcome=StatusEngine.SCHEDULED,
        formationtype__startswith="Hyke System",
        process__in=FUNCTION_MAP_STATELESS.keys()
    ))

    print("Active items in the job: " + str(len(items_state_1) + len(items_stateless)))

    db.close_old_connections()

    for item in items_state_1:
        FUNCTION_MAP_STATE_1[item.process](item)

    for item in items_stateless:
        FUNCTION_MAP_STATELESS[item.process](item)        

    print("Scheduled task is completed for Hyke System...\n")


if __name__ == "__main__":
    scheduled_system()
