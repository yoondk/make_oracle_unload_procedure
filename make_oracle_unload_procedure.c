/*
        오라클 데이타 unload를 위한 오라클 procedure 생성과 
        MariaDB에 이관 후 데이터 검증을 위한 MariaDB unload SQL 생성

        사용법 : make_utl_proc  [오라클 desc]  [테이블명]
        ex) make_utl_proc  day201_desc.txt  day201
        Input file  : day201_desc.txt
        Output file : MIG_day201.sql 
                      MIG_day201_maria.sql

        현재 DIR 설정 : EX_DIR  >> 변경필요 "line 137"
        오라클 스키마, ordery by 절 PRIMARY 정렬확인
*/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#define MAX_SIZE	32767
#define NAME_SIZE	512

FILE	*in, *out;

main(argc,argv)
int  argc;
char *argv[3];
{

	char	in_name[NAME_SIZE]  = "";
	char	out_name[NAME_SIZE] = "";
	char	tab_name[NAME_SIZE] = "";

	int	ret = -1;

	if (argc != 3)
	{
	   fprintf(stderr,"Usage : %s  COLUMN_INFO  TABLE_NAME\n",argv[0]);
	   exit(1);
	}

	strcpy(in_name,argv[1]);
	strcpy(tab_name,argv[2]);

/* Open oracle table information file */
	in = fopen(in_name,"r");
	if (in == NULL)
	{
	   fprintf(stderr,"%s file open error\n",in_name);
	   exit(1);
	}

/* output file name : MIG_{table name}.sql */
	strcpy(out_name,"MIG_");
	strcat(out_name,tab_name);
	strcat(out_name,".sql");

/* Open oracle procedure file */
	out = fopen(out_name,"w");
	if (out == NULL)
	{
	   fprintf(stderr,"%s file open error\n",out_name);
	   fclose(in);
	   exit(1);
	}

	ret = make_proc(tab_name);
        if (ret != 0) {
	   fprintf(stderr,"make_proc() : error returned...\n");
	   exit(1);
	}
	fclose(in);
	fclose(out);
/* The end of making oracle procedure */



/* Reopen oracle table information file */
	in = fopen(in_name,"r");
	if (in == NULL)
	{
	   fprintf(stderr,"%s file open error\n",in_name);
	   exit(1);
	}

/* output file name : MIG_{table name}_maria.sql */
	strcpy(out_name,"MIG_");
	strcat(out_name,tab_name);
	strcat(out_name,".sql");

/* Open maria sql file */
	out = fopen(out_name,"w");
	if (out == NULL)
	{
	   fprintf(stderr,"%s file open error\n",out_name);
	   fclose(in);
	   exit(1);
	}

	ret = make_maria_sql(tab_name);
        if (ret != 0) {
	   fprintf(stderr,"make_maria_sql() : error returned...\n");
	   exit(1);
	}
	fclose(in);
	fclose(out);

} /* end of main() */


int make_proc(char *tbl_name)
{

	char	select[MAX_SIZE]   = "";
	char	utl_file[MAX_SIZE] = "";
	int	i = 0; /* pointer for select buffer   */
	int	j = 0; /* pointer for utl_file buffer */

	char	line[MAX_SIZE] = "";

	char	col_name[NAME_SIZE] = "";
	char	col_type[NAME_SIZE] = "";

	int	null_flag = 0;		/* if type is not_null, null_flag == 1 */

	char	temp1[NAME_SIZE] = "";  /* NULL CHECK */
	char	temp2[NAME_SIZE] = "";

	char	tok1[NAME_SIZE] = "";  /* column name                  */
	char	tok2[NAME_SIZE] = "";  /* NOT         or  column type  */
	char	tok3[NAME_SIZE] = "";  /* NULL        or  Not Use      */
	char	tok4[NAME_SIZE] = "";  /* column type or  Not Use      */
	char	tok5[NAME_SIZE] = "";  /* Not Use                      */


/* Make procedure header */
	fprintf(out, "create or replace procedure %s_proc(fname varchar2) ", tbl_name);
	fprintf(out, "\n   IS  v_file UTL_FILE.FILE_TYPE; ");
	fprintf(out, "\nBEGIN ");
	fprintf(out, "\n   v_file := UTL_FILE.FOPEN_NCHAR('MIG_DIR', fname, 'W', 32767 ); ");
	fprintf(out, "\n   FOR rec IN ");

/* Make buffer(select list and loop list) header */
	sprintf(select, "\n   (SELECT ");
	i = strlen(select);

	sprintf(utl_file, "\nLOOP");
	j = strlen(utl_file);
	sprintf(&utl_file[j], "\n\tUTL_FILE.PUT_LINE_NCHAR (v_file, \'`\'||");
	j = strlen(utl_file);

	while (fgets(line, MAX_SIZE, in) != (char *)0)
	{

/* token process */
	   sscanf(line, "%s %s %s %s %s", tok1, tok2, tok3, tok4, tok5);

/* assign column name */
	   strcpy(col_name, tok1);

/* Check not null and Find column type */
	   strcpy(temp1, tok2);   /* NOT  or column type */
	   strcpy(temp2, tok3);   /* NULL or space       */

	   if ((strncmp(temp1,"NOT",3)==0)&&(strncmp(temp2,"NULL",4)==0))
	   {
	      strcpy(col_type, tok4);
	      null_flag = 1;
	   }
	   else
	   {
	      strcpy(col_type, tok2);
	      null_flag = 0;
	   }

/* Check column type */
	   if ((strncmp(col_type, "NUMBER",6)         == 0) ||
	       (strncmp(col_type, "FLOAT",5)          == 0) ||
	       (strncmp(col_type, "BINARY_FLOAT",12)  == 0) ||
	       (strncmp(col_type, "BINARY_DOUBLE",13) == 0) ||
	       (strncmp(col_type, "INT",3)            == 0))
	   {
	      sprintf(&select[i], "\n\tnvl(TO_CHAR(%s), 'NULL') \t %s ", col_name, col_name);
	      i = strlen(select);
	   }
	   else if ((strncmp(col_type, "VARCHAR",7) == 0) ||
	            (strncmp(col_type, "CLOB",4)    == 0))
	   {
	      sprintf(&select[i], "\n\tnvl(%s, 'NULL') \t %s ", col_name, col_name);
	      i = strlen(select);
	   }
	   else if ((strncmp(col_type, "CHAR",4) == 0))
	   {
	      sprintf(&select[i], "\n\tnvl(trim(%s), 'NULL') \t %s ", col_name, col_name);
	      i = strlen(select);
	   }

/* data type */
	   else if ((strncmp(col_type, "DATE",4)           == 0))
           {
	      if (null_flag == 1)
	      {
	         sprintf(&select[i], "\n\tTO_CHAR(%s,'YYYY-MM-DD HH24:MI:SS') \t %s ", col_name, col_name);
	         i = strlen(select);
	      }
	      else
	      {
	         sprintf(&select[i], "\n\tnvl(TO_CHAR(%s,'YYYY-MM-DD HH24:MI:SS'), 'NULL') \t %s ", col_name, col_name);
	         i = strlen(select);
	      }
           }

	   else
	   {
	      fprintf(stderr,"ERROR : make_proc() type mismatch %s \n", col_type);
	      exit(1);
	   }


	   sprintf(&utl_file[j], "\n\trec.%s||\'`|`\'||", col_name);
	   j = strlen(utl_file);

	   sprintf(&select[i], "," );
	   i = strlen(select);

	   null_flag = 0;

	} /* end while */

/* last comma delete in select buffer */
	if (strncmp(&select[i-1], ",", 1) == 0)
	{
	   sprintf(&select[i-1], " ");
	}

/* Make footer */
/*	sprintf(&select[i], "\nFROM  ITIMSADM.%s )", tbl_name);  */
	sprintf(&select[i], "\nFROM  %s )", tbl_name);
	i = strlen(select);

	sprintf(&utl_file[j-6], "`\');   ");
	j = strlen(utl_file);
	sprintf(&utl_file[j], "\nEND LOOP; ");
	j = strlen(utl_file);
	sprintf(&utl_file[j], "\n   UTL_FILE.FCLOSE(v_file); ");
	j = strlen(utl_file);
	sprintf(&utl_file[j], "\nEND; ");
	j = strlen(utl_file);
	sprintf(&utl_file[j], "\n/ ");
	j = strlen(utl_file);
	sprintf(&utl_file[j], "\n");
	j = strlen(utl_file);
	sprintf(&utl_file[j], "\nexec %s_proc('%s_oracle.txt'); ", tbl_name, tbl_name);
	j = strlen(utl_file);

/* Make File */
	fprintf(out, "%s %s \n", select, utl_file);

	return(0);

} /* end of make_proc() */



int make_maria_sql(char *tbl_name)
{

	char	select[MAX_SIZE]   = "";
	int	i = 0; /* pointer for select buffer   */

	char	line[MAX_SIZE] = "";

	char	col_name[NAME_SIZE] = "";
	char	col_type[NAME_SIZE] = "";

	int	null_flag = 0;		/* if type is not_null, null_flag == 1 */

	char	temp1[NAME_SIZE] = "";  /* NULL CHECK */
	char	temp2[NAME_SIZE] = "";

	char	tok1[NAME_SIZE] = "";  /* column name                  */
	char	tok2[NAME_SIZE] = "";  /* NOT         or  column type  */
	char	tok3[NAME_SIZE] = "";  /* NULL        or  Not Use      */
	char	tok4[NAME_SIZE] = "";  /* column type or  Not Use      */
	char	tok5[NAME_SIZE] = "";  /* Not Use                      */

	int     precision = 0;  /* NUMBER(P,S) */
	int     scale     = 0;  /* NUMBER(P,S) */
	int     ret = 0;   /* return value */


/* Make procedure header */
	fprintf(out, "SELECT");

/* Make buffer(select list and loop list) header */
	while (fgets(line, MAX_SIZE, in) != (char *)0)
	{
/* token process */
	   sscanf(line, "%s %s %s %s %s", tok1, tok2, tok3, tok4, tok5);

/* assign column name */
	   strcpy(col_name, tok1);

/* Check not null and Find column type */
	   strcpy(temp1, tok2);   /* NOT  or column type */
	   strcpy(temp2, tok3);   /* NULL or space       */

	   if ((strncmp(temp1,"NOT",3)==0)&&(strncmp(temp2,"NULL",4)==0))
	   {
	      strcpy(col_type, tok4);
	      null_flag = 1;
	   }
	   else
	   {
	      strcpy(col_type, tok2);
	      null_flag = 0;
	   }

/* Check column type */
	   if ((strncmp(col_type, "FLOAT",5)          == 0) ||
	       (strncmp(col_type, "BINARY_FLOAT",12)  == 0) ||
	       (strncmp(col_type, "BINARY_DOUBLE",13) == 0) ||
	       (strncmp(col_type, "INT",3)            == 0))
	   {
	      sprintf(&select[i], "\n\tnvl(%s,'NULL')   %s", col_name, col_name);
	      i = strlen(select);
	   }

	   else if (strncmp(col_type, "NUMBER",6) == 0)
	   {
	      ret = separate(col_type, &precision, &scale);
	      if (ret == 1)
	      {
	         sprintf(&select[i], "\n\tnvl(CAST(ROUND(%s,%d)AS DOUBLE),'NULL')   %s", col_name, scale - 1, col_name);
	         i = strlen(select);
	      }
	      else if (ret == 0)
	      {
	         sprintf(&select[i], "\n\tnvl(%s,'NULL')   %s", col_name, col_name);
	         i = strlen(select);
	      }
	      else
	      {
	         printf( " separate() error \n");
	         exit(1);
	      }
	   }

	   else if ((strncmp(col_type, "VARCHAR",7) == 0) ||
	            (strncmp(col_type, "CLOB",4)    == 0))
	   {
	      sprintf(&select[i], "\n\tnvl(%s,'NULL')   %s", col_name, col_name);
	      i = strlen(select);
	   }
	   else if ((strncmp(col_type, "CHAR",4) == 0))
	   {
	      sprintf(&select[i], "\n\tnvl(trim(%s),'NULL')   %s", col_name, col_name);
	      i = strlen(select);
	   }

/* date type */
	   else if ((strncmp(col_type, "DATE",4)           == 0))
           {
	      sprintf(&select[i], "\n\tnvl(%s,'NULL')   %s", col_name, col_name);
	      i = strlen(select);
           }
	   else
	   {
	      fprintf(stderr,"ERROR : make_proc() type mismatch %s \n", col_type);
	      exit(1);
	   }

	   sprintf(&select[i], "," );
	   i = strlen(select);

	   null_flag = 0;

	} /* end while */

/* last comma delete in select buffer */
	if (strncmp(&select[i-1], ",", 1) == 0)
	{
	   sprintf(&select[i-1], " ");
	}

/* Make footer */
	sprintf(&select[i], "\nFROM %s order by 1,2,3,4", tbl_name);
	i = strlen(select);

/* sprintf(&select[i], "\ninto outfile \'/maria_export/%s_maria.txt\'", tbl_name);  */
	sprintf(&select[i], "\ninto outfile  \'/MIG/%s_maria.txt\'", tbl_name);
	i = strlen(select);

	sprintf(&select[i], "\nfields terminated by \'|\'");
	i = strlen(select);

	sprintf(&select[i], "\noptionally enclosed by \'`\'");
	i = strlen(select);

	sprintf(&select[i], "\nlines terminated by \'\\n\' ;");
	i = strlen(select);


/* Make File */
	fprintf(out, "%s\n", select);

	return(0);

} /* end of make_maria_sql() */



int separate(char *number, int *precision, int *scale)
{
	char *temp1; /* NUMBER    tokenize  */
	char *temp2; /* precision tokenize  */
	char *temp3; /* scale     tokenize  */
	int  ret = 0;

	temp1 = strtok(number, "(");
	temp2 = strtok(NULL, ",");
	if (temp2 == NULL) temp2 = "22";

	temp3 = strtok(NULL, ")");
	if (temp3 == NULL)
	   temp3 = "0";
	else
	   ret++;

	*precision = atoi(temp2);
	*scale = atoi(temp3);

	return ret;

} /* end of separate() */




/* end of file */
