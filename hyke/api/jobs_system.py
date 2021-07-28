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


def scheduled_system():
    print("Scheduled task is started for Hyke System...")

    items = StatusEngine.objects.filter(Q(outcome=-1) & Q(formationtype__startswith="Hyke System"))

    print("Active items in the job: " + str(len(items)))

    db.close_old_connections()

    for item in items:
        if item.process == "Client Onboarding Survey" and item.processstate == 1 and item.outcome == -1:
            try:
                send_client_onboarding_survey(email=item.email)
            except Exception as e:
                logger.exception(f"Can't process Onboarding NPS Survey for status engine id={item.id}")

        elif item.process == "Payment error email" and item.processstate == 1 and item.outcome == -1:
            send_transactional_email(
                email=item.email, template="[Action required] - Please update your payment information",
            )
            print("[Action required] - Please update your payment information email is sent to " + item.email)
        elif item.process == "Running flow" and item.processstate == 1 and item.outcome == -1:
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
        elif item.process == "Annual Report Uploaded" and item.outcome == -1:

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
        elif item.process == "Calculate NPS Running" and item.outcome == -1:
            nps_calculator_running()

            print("Running NPS is calculated for " + item.data)
        elif item.process == "Calculate NPS Onboarding" and item.outcome == -1:
            nps_calculator_onboarding()

            print("Onboarding NPS is calculated for " + item.data)

        elif item.process == "Kickoff Questionnaire Completed" and item.processstate == 1 and item.outcome == -1:
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

        elif item.process == "Kickoff Call Scheduled" and item.processstate == 1 and item.outcome == -1:
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

        elif item.process == "Kickoff Call Cancelled" and item.processstate == 1 and item.outcome == -1:
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

        elif (
            item.process == "Transition Plan Submitted"
            and item.processstate == 1
            and item.outcome == StatusEngine.SCHEDULED
        ):
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

        elif item.process == "BK Training Call Scheduled" and item.processstate == 1 and item.outcome == -1:
            StatusEngine.objects.create(
                email=item.email,
                processstate=1,
                formationtype="Hyke Salesforce",
                outcome=-1,
                process="BK Training Call Scheduled",
                data=item.data,
            )

        elif item.process == "BK Training Call Cancelled" and item.processstate == 1 and item.outcome == -1:
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

    print("Scheduled task is completed for Hyke System...\n")


if __name__ == "__main__":
    scheduled_system()
