package com.microsoft.corfu;

import java.io.*;

@SuppressWarnings("serial")
public class CorfuException extends IOException
{
	public CorfuErrorCode er;
	
	public CorfuException(String desc)
	{
		super(desc);
	}
}


@SuppressWarnings("serial")
class OverwriteCorfuException extends CorfuException
{
	public OverwriteCorfuException(String desc)
	{
		super(desc);
		this.er = CorfuErrorCode.ERR_OVERWRITE;
	}
}

@SuppressWarnings("serial")
class TrimmedCorfuException extends CorfuException
{
	public TrimmedCorfuException(String desc)
	{
		super(desc);
		this.er = CorfuErrorCode.ERR_TRIMMED;
	}
}


@SuppressWarnings("serial")
class OutOfSpaceCorfuException extends CorfuException
{
	public OutOfSpaceCorfuException(String desc)
	{
		super(desc);
		this.er = CorfuErrorCode.ERR_FULL;
	}
}


@SuppressWarnings("serial")
class UnsupportedCorfuException extends CorfuException
{
	public UnsupportedCorfuException(String desc)
	{
		super(desc);
		this.er = CorfuErrorCode.ERR_BADPARAM;
	}
}

@SuppressWarnings("serial")
class BadParamCorfuException extends CorfuException
{
	public BadParamCorfuException(String desc)
	{
		super(desc);
		this.er = CorfuErrorCode.ERR_BADPARAM;
	}
}


@SuppressWarnings("serial")
class UnwrittenCorfuException extends CorfuException
{
	public UnwrittenCorfuException(String desc)
	{
		super(desc);
		this.er = CorfuErrorCode.ERR_UNWRITTEN;
	}
}
