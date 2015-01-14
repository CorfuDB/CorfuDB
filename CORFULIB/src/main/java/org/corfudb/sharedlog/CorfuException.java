/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.corfudb.sharedlog;

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
