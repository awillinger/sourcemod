/**
 * vim: set ts=4 :
 * =============================================================================
 * SourceMod MySQL Extension
 * Copyright (C) 2004-2008 AlliedModders LLC.  All rights reserved.
 * =============================================================================
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 3.0, as published by the
 * Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, AlliedModders LLC gives you permission to link the
 * code of this program (as well as its derivative works) to "Half-Life 2," the
 * "Source Engine," the "SourcePawn JIT," and any Game MODs that run on software
 * by the Valve Corporation.  You must obey the GNU General Public License in
 * all respects for all other code used.  Additionally, AlliedModders LLC grants
 * this exception to all derivative works.  AlliedModders LLC defines further
 * exceptions, found in LICENSE.txt (as of this writing, version JULY-31-2007),
 * or <http://www.sourcemod.net/license.php>.
 *
 * Version: $Id$
 */

#include "MyStatement.h"
#include "MyBoundResults.h"

MyStatement::MyStatement(MyDatabase *db, MYSQL_STMT *stmt)
: m_mysql(db->m_mysql), m_pParent(db), m_pRes(NULL), m_rs(NULL), m_Results(false)
{
	// Cannot use the initializer list for this, as we need a custom deleter
	m_pStmt = std::shared_ptr<MYSQL_STMT>(stmt, [](MYSQL_STMT *stmt){
		mysql_stmt_close(stmt);
	});

	m_Params = (unsigned int)mysql_stmt_param_count(m_pStmt.get());

	AllocateBindBuffers();

	m_Results = false;
}

MyStatement::MyStatement(MyStatement& other)
{
	// Copy base data
	m_mysql = other.m_mysql;

	m_pParent = ke::RefPtr<MyDatabase>(other.m_pParent);
	m_pStmt = other.m_pStmt;

	m_Params = other.m_Params;

	AllocateBindBuffers();

	// Copy parameter bindings
	if (m_Params)
	{
		// Copy raw binding structs over
		memcpy(m_bind, other.m_bind, sizeof(MYSQL_BIND) * m_Params);
		memcpy(m_pushinfo, other.m_pushinfo, sizeof(ParamBind) * m_Params);

		for (unsigned int i = 0; i < m_Params; i++)
		{
			const enum_field_types buffer_type = m_bind[i].buffer_type;

			// Update pointers so they point to the copied pushinfo structs
			switch (buffer_type)
			{
				case MYSQL_TYPE_LONG:
					m_bind[i].buffer = &(m_pushinfo[i].data.ival);
					break;
				case MYSQL_TYPE_FLOAT:
					m_bind[i].buffer = &(m_pushinfo[i].data.fval);
					break;
				case MYSQL_TYPE_STRING:
				case MYSQL_TYPE_BLOB:
				{
					const void *original_ptr = other.m_pushinfo[i].blob;
					if (original_ptr != NULL)
					{
						// If the original binding was done using a copy, we
						// also have to copy the ... copy over
						// Otherwise, the original pointer can stay as-is
						const size_t length = other.m_pushinfo[i].length;

						void *copy_ptr = malloc(length);
						memcpy(copy_ptr, original_ptr, length);

						m_pushinfo[i].blob = copy_ptr;
					}

					m_bind[i].length = &(m_bind[i].buffer_length);

					break;
				}
			}
		}
	}

	// This must also be initialized here to avoid garbage data
	m_Results = false;
}

MyStatement::~MyStatement()
{
	while (FetchMoreResults())
	{
		/* Spin until all are gone */
	}

	/* Free result set structures */
	ClearResults();

	/* Free old blobs */
	for (unsigned int i = 0; i < m_Params; i++)
	{
		free(m_pushinfo[i].blob);
	}

	/* Free our allocated arrays */
	free(m_pushinfo);
	free(m_bind);

	/* The statement pointer will automatically be closed by the shared_ptr */
}

void MyStatement::Destroy()
{
	delete this;
}

void MyStatement::AllocateBindBuffers()
{
	if (m_Params)
	{
		m_pushinfo = (ParamBind *)malloc(sizeof(ParamBind) * m_Params);
		memset(m_pushinfo, 0, sizeof(ParamBind) * m_Params);

		m_bind = (MYSQL_BIND *)malloc(sizeof(MYSQL_BIND) * m_Params);
		memset(m_bind, 0, sizeof(MYSQL_BIND) * m_Params);
	} else {
		m_pushinfo = NULL;
		m_bind = NULL;
	}
}

void MyStatement::ClearResults()
{
	if (m_rs)
	{
		delete m_rs;
		m_rs = NULL;
	}

	if (m_pRes)
	{
		mysql_free_result(m_pRes);
		m_pRes = NULL;
	}

	m_Results = false;
}

bool MyStatement::FetchMoreResults()
{
	if (m_pRes == NULL)
	{
		return false;
	}
	else if (!mysql_more_results(m_pParent->m_mysql)) {
		return false;
	}

	ClearResults();

	MYSQL_STMT *stmt = m_pStmt.get();

	if (mysql_stmt_next_result(stmt) != 0)
	{
		return false;
	}

	/* the column count is > 0 if there is a result set
	 * 0 if the result is only the final status packet in CALL queries.
	 */
	unsigned int num_fields = mysql_stmt_field_count(stmt);
	if (num_fields == 0)
	{
		return false;
	}

	/* Skip away if we don't have data */
	m_pRes = mysql_stmt_result_metadata(stmt);
	if (!m_pRes)
	{
		return false;
	}

	/* If we don't have a result manager, create one. */
	if (!m_rs)
	{
		m_rs = new MyBoundResults(stmt, m_pRes, num_fields);
	}

	/* Tell the result set to update its bind info,
	* and initialize itself if necessary.
	*/
	if (!(m_Results = m_rs->Initialize()))
	{
		return false;
	}

	/* Try precaching the results. */
	m_Results = (mysql_stmt_store_result(stmt) == 0);

	/* Update now that the data is known. */
	m_rs->Update();

	/* Return indicator */
	return m_Results;
}

void *MyStatement::CopyBlob(unsigned int param, const void *blobptr, size_t length)
{
	void *copy_ptr = NULL;

	if (m_pushinfo[param].blob != NULL)
	{
		if (m_pushinfo[param].length < length)
		{
			free(m_pushinfo[param].blob);
		} else {
			copy_ptr = m_pushinfo[param].blob;
		}
	}

	if (copy_ptr == NULL)
	{
		copy_ptr = malloc(length);
		m_pushinfo[param].blob = copy_ptr;
		m_pushinfo[param].length = length;
	}

	memcpy(copy_ptr, blobptr, length);

	return copy_ptr;
}

bool MyStatement::BindParamInt(unsigned int param, int num, bool signd)
{
	if (param >= m_Params)
	{
		return false;
	}

	m_pushinfo[param].data.ival = num;
	m_bind[param].buffer_type = MYSQL_TYPE_LONG;
	m_bind[param].buffer = &(m_pushinfo[param].data.ival);
	m_bind[param].is_unsigned = signd ? 0 : 1;
	m_bind[param].length = NULL;

	return true;
}

bool MyStatement::BindParamFloat(unsigned int param, float f)
{
	if (param >= m_Params)
	{
		return false;
	}

	m_pushinfo[param].data.fval = f;
	m_bind[param].buffer_type = MYSQL_TYPE_FLOAT;
	m_bind[param].buffer = &(m_pushinfo[param].data.fval);
	m_bind[param].length = NULL;

	return true;
}

bool MyStatement::BindParamString(unsigned int param, const char *text, bool copy)
{
	if (param >= m_Params)
	{
		return false;
	}

	const void *final_ptr;
	size_t len;

	if (copy)
	{
		len = strlen(text);
		final_ptr = CopyBlob(param, text, len+1);
	} else {
		len = strlen(text);
		final_ptr = text;
	}

	m_bind[param].buffer_type = MYSQL_TYPE_STRING;
	m_bind[param].buffer = (void *)final_ptr;
	m_bind[param].buffer_length = (unsigned long)len;
	m_bind[param].length = &(m_bind[param].buffer_length);

	return true;
}

bool MyStatement::BindParamBlob(unsigned int param, const void *data, size_t length, bool copy)
{
	if (param >= m_Params)
	{
		return false;
	}

	const void *final_ptr;
	
	if (copy)
	{
		final_ptr = CopyBlob(param, data, length);
	} else {
		final_ptr = data;
	}

	m_bind[param].buffer_type = MYSQL_TYPE_BLOB;
	m_bind[param].buffer = (void *)final_ptr;
	m_bind[param].buffer_length = (unsigned long)length;
	m_bind[param].length = &(m_bind[param].buffer_length);

	return true;
}

bool MyStatement::BindParamNull(unsigned int param)
{
	if (param >= m_Params)
	{
		return false;
	}

	m_bind[param].buffer_type = MYSQL_TYPE_NULL;

	return true;
}

IPreparedQuery *MyStatement::Clone()
{
	return new MyStatement(*this);
}

bool MyStatement::Execute()
{
	/* Clear any past result first! */
	while (FetchMoreResults())
	{
		/* Spin until all are gone */
	}

	/* Free result set structures */
	ClearResults();

	MYSQL_STMT *stmt = m_pStmt.get();

	/* Bind the parameters */
	if (m_Params)
	{
		if (mysql_stmt_bind_param(stmt, m_bind) != 0)
		{
			return false;
		}
	}

	if (mysql_stmt_execute(stmt) != 0)
	{
		return false;
	}

	/* the column count is > 0 if there is a result set
	 * 0 if the result is only the final status packet in CALL queries.
	 */
	unsigned int num_fields = mysql_stmt_field_count(stmt);
	if (num_fields == 0)
	{
		return true;
	}

	/* Skip away if we don't have data */
	m_pRes = mysql_stmt_result_metadata(stmt);
	if (!m_pRes)
	{
		return true;
	}

	/* Create our result manager. */
	m_rs = new MyBoundResults(stmt, m_pRes, num_fields);

	/* Tell the result set to update its bind info,
	 * and initialize itself if necessary.
	 */
	if (!(m_Results = m_rs->Initialize()))
	{
		return false;
	}

	/* Try precaching the results. */
	m_Results = (mysql_stmt_store_result(stmt) == 0);

	/* Update now that the data is known. */
	m_rs->Update();

	/* Return indicator */
	return m_Results;
}

IDatabase *MyStatement::GetDatabase()
{
	return m_pParent.get();
}

const char *MyStatement::GetError(int *errCode/* =NULL */)
{
	MYSQL_STMT *stmt = m_pStmt.get();

	if (errCode)
	{
		*errCode = mysql_stmt_errno(stmt);
	}

	return mysql_stmt_error(stmt);
}

unsigned int MyStatement::GetAffectedRows()
{
	return (unsigned int)mysql_stmt_affected_rows(m_pStmt.get());
}

unsigned int MyStatement::GetInsertID()
{
	return (unsigned int)mysql_stmt_insert_id(m_pStmt.get());
}

IResultSet *MyStatement::GetResultSet()
{
	return (m_Results ? m_rs : NULL);
}
