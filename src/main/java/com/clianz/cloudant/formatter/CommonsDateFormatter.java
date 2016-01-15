package com.clianz.cloudant.formatter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class CommonsDateFormatter {

	public static final String APACHE_COMMONS_DT_FORMAT = "yyyy/MM/dd HH:mm:ss:SSS zzz";

	private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

	public static String toDateString(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat(APACHE_COMMONS_DT_FORMAT);
		sdf.setTimeZone(UTC);
		return sdf.format(date);
	}
}
