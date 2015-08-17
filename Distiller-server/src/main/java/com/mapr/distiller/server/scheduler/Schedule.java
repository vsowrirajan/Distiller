package com.mapr.distiller.server.scheduler;

public class Schedule {
	long lastScheduledStartTime, lastStartTime, lastEndTime, periodicity;
	double maxDurationPct = 0.2d;
	double minWaitPct = 0.5d;
	
	public void setTimestamps(long lastStartTime, long lastEndTime){
		this.lastStartTime = lastStartTime;
		this.lastEndTime = lastEndTime;
	}
	
	public Schedule(long periodicity, double maxDurationPct, double minWaitPct)
							throws Exception
	{
		if(periodicity < 1000) 
			throw new Exception("Minimum scheduling periodicity is 1000 (ms), value provided: " + periodicity);
		if(maxDurationPct<0d | maxDurationPct>1d)
			throw new Exception("Invalid maxDurationPct: " + maxDurationPct + ", expected 0 <= maxDurationPct <= 1");
		if(minWaitPct<0d | minWaitPct>1d)
			throw new Exception("Invalid maxDurationPct: " + maxDurationPct + ", expected 0 <= maxDurationPct <= 1");
		if(maxDurationPct + minWaitPct > 1d)
			throw new Exception("Combined value of maxDurationPct and minWaitPct must be <= 1, minWaitPct: " + minWaitPct +" maxDurationPct: " + maxDurationPct);
		
		this.lastScheduledStartTime=-1l;
		this.lastStartTime = -1l;
		this.lastEndTime = -1l;
		this.periodicity = periodicity;
	}
	public long getNextTime() throws Exception{
		if(lastScheduledStartTime==-1)
			lastScheduledStartTime = System.currentTimeMillis();
		return lastScheduledStartTime;
	}
	
	public void advanceSchedule() throws Exception{
		
		if
		( ( lastStartTime != -1 || 
			lastEndTime != -1
		  ) 
		  &&
		  ( lastStartTime < lastScheduledStartTime || 
			lastEndTime < lastStartTime
		  )
		)
			throw new Exception("Invalid timestamps.  lastScheduledStartTime:" + lastScheduledStartTime + 
								" lastStartTime:" + lastStartTime + " lastEndTime:" + lastEndTime);
		
		long duration = lastEndTime - lastStartTime;
		long idealNextScheduledStartTime = lastScheduledStartTime + periodicity;
		long waitTimeForIdealStartTime = idealNextScheduledStartTime - System.currentTimeMillis();
		double waitTimePct = (double)waitTimeForIdealStartTime / (double)periodicity;
		double durationPct = (double)duration / (double)periodicity;
		if(durationPct > maxDurationPct){
			long adjustedPeriodicity = (long)((double)duration / maxDurationPct);
			if (lastScheduledStartTime + adjustedPeriodicity < 
				lastEndTime + (long)((double)adjustedPeriodicity * minWaitPct)) {
				lastScheduledStartTime = lastEndTime + (long)((double)adjustedPeriodicity * minWaitPct);
			} else {
				lastScheduledStartTime += adjustedPeriodicity;
			}
		} else {
			if(waitTimePct < minWaitPct){
				lastScheduledStartTime = lastEndTime + ((long)( minWaitPct * (double)periodicity));
			} else {
				lastScheduledStartTime = idealNextScheduledStartTime;
			}
		}
	}
}
