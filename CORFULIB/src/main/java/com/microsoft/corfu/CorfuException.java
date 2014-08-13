package com.microsoft.corfu;

import java.io.*;

@SuppressWarnings("serial")
public class CorfuException extends IOException
{
	public ErrorCode er;
	
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
		this.er = ErrorCode.ERR_OVERWRITE;
	}
}

@SuppressWarnings("serial")
class TrimmedCorfuException extends CorfuException
{
	public TrimmedCorfuException(String desc)
	{
		super(desc);
		this.er = ErrorCode.ERR_TRIMMED;
	}
}


@SuppressWarnings("serial")
class OutOfSpaceCorfuException extends CorfuException
{
	public OutOfSpaceCorfuException(String desc)
	{
		super(desc);
		this.er = ErrorCode.ERR_FULL;
	}
}


@SuppressWarnings("serial")
class UnsupportedCorfuException extends CorfuException
{
	public UnsupportedCorfuException(String desc)
	{
		super(desc);
		this.er = ErrorCode.ERR_BADPARAM;
	}
}

@SuppressWarnings("serial")
class BadParamCorfuException extends CorfuException
{
	public BadParamCorfuException(String desc)
	{
		super(desc);
		this.er = ErrorCode.ERR_BADPARAM;
	}
}


@SuppressWarnings("serial")
class UnwrittenCorfuException extends CorfuException
{
	public UnwrittenCorfuException(String desc)
	{
		super(desc);
		this.er = ErrorCode.ERR_UNWRITTEN;
	}
}

@SuppressWarnings("serial")
class InternalCorfuException extends CorfuException
{
	public InternalCorfuException(String desc)
	{
		super(desc);
		this.er = ErrorCode.ERR_BADPARAM;
	}
}


@SuppressWarnings("serial")
class ConfigCorfuException extends CorfuException
{
    public ConfigCorfuException(String desc)
    {
        super(desc);
        this.er = ErrorCode.ERR_STALEEPOCH;
    }
}
