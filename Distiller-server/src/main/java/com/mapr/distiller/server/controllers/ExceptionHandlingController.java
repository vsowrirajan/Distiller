package com.mapr.distiller.server.controllers;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.mapr.distiller.common.status.ErrorInfo;

@ControllerAdvice
public class ExceptionHandlingController {

	private static final Logger LOG = LoggerFactory
			.getLogger(ExceptionHandlingController.class);

	@ResponseStatus(HttpStatus.BAD_REQUEST)
	@ExceptionHandler(value = Exception.class)
	public @ResponseBody ErrorInfo handleError(HttpServletRequest req,
			Exception exception) {
		LOG.error("Request: " + req.getRequestURL() + " raised " + exception);
		return new ErrorInfo(req.getRequestURL().toString(), exception);
	}
}
