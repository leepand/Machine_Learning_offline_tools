/*等值分箱*/
%macro BinEqW2(dsin,var,Nb,dsout,Map);
proc sql noprint;
	select max(&var) into :Vmax from &dsin;
	select min(&Var) into :Vmin from &dsin;
quit;
%let Bs=%sysevalf((&Vmax-&Vmin)/&Nb);
data &dsout;
	set &dsin;
%do i=1 %to &Nb;
	%let Bin_U=%sysevalf(&Vmin+&i*&Bs);
	%let Bin_L=%sysevalf(&Bin_U-&Bs);
	%if &i=1 %then %do;
		IF &var>=&Bin_L and &var<=&Bin_U THEN &var._Bin=&i;
	%end;
	%else %if &i>1 %then %do;
		IF &var>&Bin_L and &var<=&Bin_U THEN &var._Bin=&i;
	%end;
%end;
run;
proc sql noprint;
create table &Map (BinMin num, BinMax num, BinNo num);
	%do i=1 %to &Nb;
		%let Bin_U=%sysevalf(&Vmin+&i*&Bs);
		%let Bin_L=%sysevalf(&Bin_U-&Bs);
		insert into &Map values(&Bin_L,&Bin_U,&i);
	%end;
quit;
%mend;
/* 获取分类变量的比率和数量*/
%macro CalcCats(DSin,Var,DSCats);
proc freq data=&DSin noprint ;
	tables &Var /missing out=&DSCats;
run;
%mend;
/*存储分类index*/
%macro Decompose(List,list_store);
proc sql noprint;
	create table &list_store (Category char(10));
quit;
%let i=1;
%let condition = 0;
%do %until (&condition =1);
	%let Word=%scan(&List,&i);
	%if &Word = %then %let condition =1;
		%else %do;
%put &Word;
			proc sql;
				insert into &list_store values ("&word");
			quit;
%let i = %Eval(&i+1);
		%end; /* end of the do */
%end; /* end of the until loop */
%mend;
/*寻找最优分割位置*/
%macro GSplit(Listin,DSFreqs,M_ListL,M_ListR,M_GiniRatio);
%Decompose(&Listin,Temp_GCats);
data _null_;
	set Temp_GCats;
	call symput("M", compress(_N_));
	call symput("C_"||left(_N_),compress(Category));
run;
proc sql noprint;
%let NL=0; %let N1=0; %let N0=0;
%do j=1 %to &M;
select DV1,DV0,Ni into:DV1_&j,:DV0_&j,:Ni_&j
	from &DSFreqs where Category="&&C_&j";
	%let NL=%Eval(&NL+&&Ni_&j);
	%let N1=%eval(&N1+&&DV1_&j);
	%let N0=%eval(&N0+&&DV0_&j);
%end;
%let GL=%sysevalf(1-(&N1*&N1+&N0*&N0)/(&NL*&NL));
quit;
/*计算Gini ratio找出其最大值和位置*/
%let MaxRatio=0;
%let BestSplit=0;
%do Split=1 %to %eval(&M-1);
/* The left node contains nodes from 1 to Split */
	%let DV1_L=0;
	%let DV0_L=0;
	%let N_L=0;
%do i=1 %to &Split;
	%let DV1_L=%eval(&DV1_L+&&DV1_&i);
	%let DV0_L=%eval(&DV0_L+&&DV0_&i);
	%let N_L=%eval(&N_L+&&Ni_&i);
%end;
/* The right node contains nodes from Split+1 to M */
%let DV1_R=0;
%let DV0_R=0;
%let N_R=0;
%do i=%eval(&Split+1) %to &M;
	%let DV1_R=%eval(&DV1_R+&&DV1_&i);
	%let DV0_R=%eval(&DV0_R+&&DV0_&i);
	%let N_R=%eval(&N_R+&&Ni_&i);
%end;
%let G_L = %sysevalf(1-(&DV1_L*&DV1_L+&DV0_L*&DV0_L)
/(&N_L*&N_L));
%let G_R = %sysevalf(1-(&DV1_R*&DV1_R+&DV0_R*&DV0_R)
/(&N_R*&N_R)) ;
%let G_s= %sysevalf( (&N_L * &G_L + &N_R * &G_R)/&NL);
%let GRatio = %sysevalf(1-&G_s/&GL);
%if %sysevalf(&GRatio>&MaxRatio) %then %do;
	%let BestSplit=&Split;
	%let MaxRatio=&Gratio;
%end;
%end;
/* The left list is: */
%let ListL =;
%do i=1 %to &BestSplit;
	%let ListL = &ListL &&C_&i;
%end;
/* and the right list is: */
%let ListR=;
%do i=%eval(&BestSplit+1) %to &M;
	%let ListR = &ListR &&C_&i;
%end;
/* Return the output values */
%let &M_GiniRatio=&MaxRatio;
%let &M_ListL=&ListL;
%let &M_ListR = &ListR;
%mend ;
%macro GBinBDV(DSin,IVVar,DVVar,NW,Mmax,DSGroups,DSVarMap);
/* 用Gini变量考虑因哑变量规约（bin）连续变量：
DSin=输入数据集
IVVar=将要bin的连续自变量
DVVar=因哑变量
NW=初始分割数，基于等距离bin，注：NW的值设置的越大，得到的结果越精确
，缺点：计算成本增加.
MMax=使用者需要的分割数
DSGroups=初始和最终分割对比
DSVarMap=映射规则
分割前需处理缺失值*/
/* 为方便后面处理，先重命名源数据中的分割变量和因变量 */
Data Temp_D;
	set &DSin (rename=(&IVVar=IV &DVVAR=DV));
	keep IV DV;
run;
%BinEqW2(Temp_D,IV,&NW,Temp_B,Temp_BMap);
%CalcCats(TEMP_B,IV_Bin,Temp_Cats);
proc sort data=Temp_Cats;
	by IV_Bin;
run;
Data _null_;
	set Temp_Cats;
	call symput ("C_"||left(_N_),compress(IV_Bin));
	call symput ("n_"||left(_N_),left(count));
	call symput ("M",left(_N_));
run;
proc sql noprint;
	create table Temp_Freqs (Category char(50),DV1 num,DV0 num,
	Ni num,P1 num );
%do i=1 %to &M;
	select count(IV) into :n1 from Temp_B
		where IV_Bin=&&C_&i and DV=1;
	select count(IV) into :n0 from Temp_B
		where IV_Bin = &&C_&i and DV=0;
%let p=%sysevalf(&n1/&&n_&i);
		insert into Temp_Freqs values("&&C_&i",&n1,&n0,
	&&n_&i,&p);
%end;
quit;
/*将分类值存入node中*/
data Temp_TERM;
	length node $1000;
	Node='';
%do j=1 %to &M;
	Node=Node||"&&C_&j";
%end;
run;
%let NNodes=1;
%DO %WHILE (&NNodes<&MMax);
Data _Null_;
	set Temp_TERM;
call symput ("L_"||left(_N_), Node);
run;
%let BestRatio=0;
%DO inode=1 %to &NNodes;
%let List_L=; %let List_R=; %Let GiniRatio=;
%GSplit(&&L_&inode,Temp_Freqs,List_L,List_R,GiniRatio);
/* Compare the GiniRatio */
%if %sysevalf(&GiniRatio>&BestRatio) %then %do;
	%let BestRatio=&GiniRatio;
	%let BestLeft=&List_L;
	%let BestRight=&List_R;
	%let BestNode=&Inode;
%end;
%End; /* end of the current node list */
Data Temp_TERM;
	Set Temp_TERM;
	if _N_=&BestNode Then delete;
run;
proc sql noprint;
	insert into Temp_TERM values ("&BestLeft");
	insert into Temp_TERM values ("&BestRight");
quit;
%let NNodes=%Eval(&NNodes +1);
%END; /* End of the splitting loop */
data _NULL_;
Set Temp_TERM;
	call symput("List_"||left(_N_),Node);
	call symput("NSplits",compress(_N_));
run;
proc sql noprint;
	create table &DSVarMap (BinMin num, BinMax num, BinNo num);
quit;
%DO ix=1 %to &NSplits;
data term_temp;
	set Temp_term;
	first=input(scan(Node,1),F10.0);
	last=input(scan(Node,-1),F10.0);
run;
data _null_;
	set term_temp;
	call symput("first_"||left(_N_),first);
	call symput("last_"||left(_n_),last);
run;
proc sql noprint;
	select BinMin into :Bmin_F from Temp_BMap
		where BinNo=&&First_&ix;
	select BinMax into :Bmax_L from Temp_BMap
		where BinNo=&&Last_&ix;
	insert into &DSVarMap values (&Bmin_F, &Bmax_L, &ix);
quit;
%END;
/* Generate DSGroups */
data Temp_TERM;
	set Temp_TERM;
	first=input(scan(Node,1),F10.0);
run;
proc sort data=Temp_TERM;
	by first;
run;
Data &DSGroups;
	set Temp_TERM (Rename=(Node=OldBin));
	NewBin=_N_;
	drop first;
run;
proc sort data=&DSVarMap;
	by BinMin;
run;
/* and regenerate the values of BinNo accordingly. */
data &DSVarMap;
	Set &DsVarMap;
	BinNo=_N_;
run;
%mend;

%macro AppBins(DSin,XVar,DSVarMap,XTVar,DSout);
/*根据GBinBDV中产生的DSVarMap映射集将XVar重新bin，并将结果存入dsout中
转换后的变量为XTVar. */
data _Null_;
	set &DSVarMap;
	call symput ("Cmin_"||left(_N_),compress(BinMin));
	call symput ("Cmax_"||left(_N_),compress(Binmax));
	call symput ("M",compress(_N_));
run;
Data &DSout;
	SET &DSin;
/* The condition loop */
IF &Xvar<=&Cmax_1 THEN &XTVar=1;
	%do i=2 %to %eval(&M-1);
		IF &XVar>&&Cmin_&i AND &XVar<=&&Cmax_&i
			THEN &XTVar=&i ;
	%end;
IF &Xvar>&&Cmin_&M THEN &XTVar=&M;
run;
%mend;


