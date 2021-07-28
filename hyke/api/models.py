from django.db import models
from django.utils import timezone
from simple_history.models import HistoricalRecords
import datetime


class ProgressStatus(models.Model):
    PENDING = "pending"
    COMPLETED = "completed"

    email = models.CharField(max_length=100, default="---")
    llcformationstatus = models.CharField(max_length=50, default="---")
    postformationstatus = models.CharField(max_length=50, default="---")
    einstatus = models.CharField(max_length=50, default="---")
    businesslicensestatus = models.CharField(max_length=50, default="---")
    bankaccountstatus = models.CharField(max_length=50, default="---")
    contributionstatus = models.CharField(max_length=50, default="---")
    SOIstatus = models.CharField(max_length=50, default="---")
    FTBstatus = models.CharField(max_length=50, default="---")
    questionnairestatus = models.CharField(max_length=50, default="---")
    bookkeepingsetupstatus = models.CharField(max_length=50, default="---")
    taxsetupstatus = models.CharField(max_length=50, default="---")
    clientsurveystatus = models.CharField(max_length=50, default="---")
    bk_services_setup_status = models.CharField(
        max_length=50, choices=[(PENDING, PENDING), (COMPLETED, COMPLETED),], default=PENDING
    )

    history = HistoricalRecords()

    class Meta:
        verbose_name = "ProgressStatus"
        verbose_name_plural = "ProgressStatuses"

    def __str__(self):
        return "%s - %s" % (self.id, self.email)


class StatusEngine(models.Model):
    FAILED = -4
    SECOND_RETRY = -3
    FIRST_RETRY = -2
    SCHEDULED = -1
    COMPLETED = 1
    UNNECESSARY = 4
    OFFBOARDED = 5

    OUTCOMES = [
        (SCHEDULED, "Scheduled"),
        (COMPLETED, "Completed"),
        (UNNECESSARY, "Cancelled due to Completed Task"),
        (OFFBOARDED, "Cancelled due to Offboarding"),
        (FIRST_RETRY, "Retrying previously failed"),
        (SECOND_RETRY, "Retrying previously failed again"),
        (FAILED, "Gave up retrying due to multiple failures"),
    ]

    email = models.CharField(max_length=50, blank=True)
    process = models.CharField(max_length=100)
    formationtype = models.CharField(max_length=20, default="---")
    processstate = models.IntegerField(default=1)
    outcome = models.IntegerField(choices=OUTCOMES, default=SCHEDULED)
    data = models.CharField(max_length=1000, default="---")
    created = models.DateTimeField(default=timezone.now)
    executed = models.DateTimeField(null=True, blank=True)

    class Meta:
        verbose_name = "StatusEngine"
        verbose_name_plural = "StatusEngines"

    def __str__(self):
        return "%s - %s - %s" % (self.id, self.email, self.process)


class ScheduledCalendlyLogManager(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(is_canceled=False)


class CalendlyLog(models.Model):
    created = models.DateTimeField(default=timezone.now)
    updated = models.DateTimeField(auto_now=True)
    email = models.CharField(max_length=100, default="---", db_index=True)
    phonenumber = models.CharField(max_length=100, default="---")
    slug = models.CharField(max_length=100, default="---")
    event_name = models.CharField(max_length=200, default="---")
    assignedto = models.CharField(max_length=100, default="---")
    eventtype = models.CharField(max_length=100, default="---")
    scheduledtime = models.DateTimeField(default=None, null=True, blank=True)
    is_canceled = models.BooleanField(default=False, blank=True)
    event_id = models.CharField(max_length=50, default="---", unique=True, db_index=True)
    data = models.CharField(max_length=10000, default="---")
    history = HistoricalRecords()
    objects = models.Manager()
    scheduled = ScheduledCalendlyLogManager()

    class Meta:
        verbose_name = "CalendlyLog"
        verbose_name_plural = "CalendlyLogs"

    def __str__(self):
        pretty_print_time = datetime.strftime(self.scheduledtime, "%I:%M%p - %A, %B %d, %Y")
        return "%s - %s - %s - %s" % (self.id, self.email, self.slug, pretty_print_time)
