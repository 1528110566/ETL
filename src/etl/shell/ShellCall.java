package etl.shell;

import db.advanced.DBOperateAdvanced;
import db.advanced.DBOperateAdvancedImpl;
import exception.parameter.ParameterUseErrorException;
import exception.parameter.UnSupportedFunctionException;
import oracle.jdbc.OracleType;
import system.parameter.FunctionParameter;
import system.parameter.ProcedureParameter;
import tool.Tools;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static etl.log.Log.log_stat;
import static etl.log.Log.log_step;
import static system.function.Function.decode;

/**
 * @author TZ
 * @date 2019/8/22 16:18
 * 调用Job
 */
public class ShellCall {
    private static DBOperateAdvanced dbOperate = new DBOperateAdvancedImpl();
    /**
     * 作业成功标志
     */
    private static String O_SUCC_FLAG;
    /**
     * 外壳成功标志
     */
    private static String O_SHELL_FLAG;
    private String V_BATCH_ID; // 批次号
    private String V_SHELL_NAME; // 外壳名称
    private String V_RUN_STRING; // 运行字符串

    /**
     * 外壳调用标准化ETL配置信息、标准存储过程的外壳，传递参数、调用存储过程、时间翻牌
     *
     * @param I_JOB_NAME 作业名（必输）
     * @param I_BATCH_ID 批次号
     */
    public void call(String I_JOB_NAME, String I_BATCH_ID) throws SQLException, UnSupportedFunctionException, ParameterUseErrorException {
        try {
            String V_BEGIN_TIME; // 增量开始时间
            String V_END_TIME; // 增量终止时间
            int V_ALL_FLAG; // 全量标志
            String V_JOB_STEP; // 作业步骤
            String V_PROC_TYPE; // 标准化ETL处理方式
            String V_CMP_TYPE; // 补差异处理方式
            int V_ENABLE_FLAG; // 启用标志
            String V_SUCC_FLAG = null; // 成功标志
            String V_JOB_TYPE; // 作业类型
            String V_OBJECT_STAT; // 对象状态
            int V_NUMBER; // 计数
            String V_RELA_TYPE; // 关联更新类型(OLD/NEW)
            String V_CURRENT_USER;

            CallableStatement statement = null;
            // 如果批次号为空，使用SEQ_CTL_BATCH
            if (I_BATCH_ID == null || "".equals(I_BATCH_ID)) {
                V_BATCH_ID = Tools.getSequenceNextValue("SEQ_CTL_BATCH");
            } else {
                V_BATCH_ID = I_BATCH_ID;
            }
            V_SHELL_NAME = "【P_ETL_SHELL_CALL】";
            V_JOB_STEP = V_SHELL_NAME + "：【调度外壳开始运行】";
            log_step(I_JOB_NAME, V_BATCH_ID, V_JOB_STEP, "【调度外壳开始运行】", null, null, null, "1", 0, 0);

            // 记录开始状态、初始化作业的控制参数
            log_stat(I_JOB_NAME, "2", V_BATCH_ID);
            dbOperate.commit();

            V_CURRENT_USER = dbOperate.callFunction("", "SYS_CONTEXT",
                    new FunctionParameter(false, OracleType.VARCHAR2),
                    new FunctionParameter(true, "USERENV"),
                    new FunctionParameter(true, "CURRENT_USER")).getString(1);
            // 获取作业启用标志,如果作业启用标志为0，则直接结束
            // 获取作业类型，不同的作业类型，使用不同的方法
            // 1为小时、2为天、3为周、4为月、5为季、6为半年、7为年、0为最新时间
            ResultSet resultSet = dbOperate.select(
                    "select ENABLE_FLAG, JOB_TYPE from T_CTL_JOB_INFO where job_name = '" + I_JOB_NAME + "'");
            resultSet.next();
            V_ENABLE_FLAG = resultSet.getInt(1);
            V_JOB_TYPE = resultSet.getString(2);

            resultSet = dbOperate.select(
                    "select TO_CHAR(INC_BEGIN_DATE, 'yyyy-mm-dd hh24:mi:ss'), " +
                            "TO_CHAR(INC_END_DATE, 'yyyy-mm-dd hh24:mi:ss'), ALL_FLAG from " +
                            "T_CTL_LOG_STAT   where job_name = '" + I_JOB_NAME + "' and BATCH_ID = '" + V_BATCH_ID + "'");
            resultSet.next();
            V_BEGIN_TIME = resultSet.getString(1);
            V_END_TIME = resultSet.getString(2);
            V_ALL_FLAG = resultSet.getInt(3);
            if (V_ENABLE_FLAG == 0) {
                raiseResultException(I_JOB_NAME, V_BATCH_ID, V_SHELL_NAME);
            }

            // 获取作业对应的标准化ETL相关的处理方法,不同的方式会调用不同的存储过程
            resultSet = dbOperate.select(
                    "select max(PROC_TYPE) from T_ETL_TAB_CONF where JOB_NAME = '" + I_JOB_NAME + "'");
            resultSet.next();
            V_PROC_TYPE = resultSet.getString(1);
            V_RUN_STRING = (String) decode(V_ALL_FLAG, 1, "【全量抽取】",
                    "【增量抽取】【" + V_BEGIN_TIME + " -> " + V_END_TIME + "】");
            V_JOB_STEP = V_SHELL_NAME + "：【调度外壳】：作业初始化参数";
            log_step(I_JOB_NAME, V_BATCH_ID, V_JOB_STEP,
                    V_RUN_STRING, null, null, null, "1");
            dbOperate.commit();

            // 外壳调度前续进程，存在相关配置才运行20180412
            resultSet = dbOperate.select("SELECT COUNT(1)" +
                    "      FROM T_CTL_JOB_FOLLOW T, T_CTL_FOLLOW T1" +
                    "     WHERE T.JOB_NAME = '" + I_JOB_NAME + "'" +
                    "       AND T.ENABLE_FLAG = '1'" +
                    "       AND T.FOLLOW_TYPE = T1.FOLLOW_TYPE" +
                    "       AND T1.FOLLOW_RUN_SCENE = decode(T1.FOLLOW_RUN_SCENE,2,T1.FOLLOW_RUN_SCENE,'" + V_ALL_FLAG + "')" +
                    "       AND T1.FOLLOW_FLAG = '1'" +
                    "       AND T1.ENABLE_FLAG = '1'");
            resultSet.next();
            V_NUMBER = resultSet.getInt(1);
            // 运行前续进程
            if (V_NUMBER > 0) {
                V_JOB_STEP = V_SHELL_NAME + "：【调度外壳】：执行前续进程";
                log_step(I_JOB_NAME, V_BATCH_ID, V_JOB_STEP, "【运行成功】", null, null, null, "1");
                statement = dbOperate.callProcedure("PKG_CTL_FOLLOW", "P_CTL_FOLLOW_MAIN",
                        new ProcedureParameter(true, I_JOB_NAME),
                        new ProcedureParameter(true, V_BATCH_ID),
                        new ProcedureParameter(true, "1"),
                        new ProcedureParameter(false, OracleType.VARCHAR2));
                O_SUCC_FLAG = statement.getString(4);
                if ("0".equals(O_SHELL_FLAG)) {
                    V_JOB_STEP = V_SHELL_NAME + "：【调度外壳】：前续进程存在异常";
                    log_step(I_JOB_NAME, V_BATCH_ID, V_JOB_STEP, "【运行成功】", null, null, null, "1");
                    raiseResultFollowException(I_JOB_NAME, V_BATCH_ID, V_SHELL_NAME);
                }
            }
            ProcedureParameter[] parameters = {
                    new ProcedureParameter(true, I_JOB_NAME),
                    new ProcedureParameter(true, V_BEGIN_TIME),
                    new ProcedureParameter(true, V_END_TIME),
                    new ProcedureParameter(true, V_ALL_FLAG),
                    new ProcedureParameter(true, V_BATCH_ID),
                    new ProcedureParameter(false, OracleType.VARCHAR2)};
            // 外壳调度标准化ETL相关存储过程;
            // 作业类型（1标准存储过程、2标准化ETL、3数据补差异）
            // 当作业类型为标准化ETL，使用包PKG_ETL_TAB
            if ("2".equals(V_JOB_TYPE)) {
                switch (V_PROC_TYPE) {
                    case "0":
                        // 当处理方式为0时，运行PKG_ETL_TAB.P_ETL_TAB_DATE（业务日期方式）
                        statement = dbOperate.callProcedure("PKG_ETL_TAB", "P_ETL_TAB_DATE", parameters);
                        V_SUCC_FLAG = statement.getString(6);
                        break;
                    case "1":
                        // 当处理方式为1时，运行PKG_ETL_TAB.P_ETL_TAB_TIME（时间戳方式）
                        statement = dbOperate.callProcedure("PKG_ETL_TAB", "P_ETL_TAB_TIME", parameters);
                        V_SUCC_FLAG = statement.getString(6);
                        break;
                    case "2":
                        // 当处理方式为2时，运行PKG_ETL_TAB.P_ETL_TAB_XHDSJ（时间戳方式）
                        statement = dbOperate.callProcedure("PKG_ETL_TAB", "P_ETL_TAB_XHDSJ", parameters);
                        V_SUCC_FLAG = statement.getString(6);
                        break;
                    case "3":
                        // 当处理方式为3时，运行PKG_ETL_TAB.P_ETL_TAB_VIEW（物化视图模式）
                        statement = dbOperate.callProcedure("PKG_ETL_TAB", "P_ETL_TAB_VIEW", parameters);
                        V_SUCC_FLAG = statement.getString(6);
                        break;
                    case "4":
                        // 当处理方式为4时，运行PKG_ETL_TAB.P_ETL_TAB_SCRIPT（自定义SQL模式）
                        statement = dbOperate.callProcedure("PKG_ETL_TAB", "P_ETL_TAB_SCRIPT", parameters);
                        V_SUCC_FLAG = statement.getString(6);
                        break;
                    case "5":
                        // 当处理方式为5时，运行PKG_ETL_TAB.P_ETL_TAB_REFRESH（刷新物化视图）
                        statement = dbOperate.callProcedure("PKG_ETL_TAB", "P_ETL_TAB_REFRESH", parameters);
                        V_SUCC_FLAG = statement.getString(6);
                        break;
                    case "6":
                        // 当处理方式为5时，运行PKG_ETL_TAB.P_ETL_TAB_EXTRACT（外部加载模式）
                        statement = dbOperate.callProcedure("PKG_ETL_TAB", "P_ETL_TAB_EXTRACT", parameters);
                        V_SUCC_FLAG = statement.getString(6);
                        break;
                    case "7":
                        // 当处理方式为5时，运行PKG_ETL_TAB.P_ETL_TAB_YXSB（有效申报模式）
                        statement = dbOperate.callProcedure("PKG_ETL_TAB", "P_ETL_TAB_YXSB", parameters);
                        V_SUCC_FLAG = statement.getString(6);
                        break;
                    case "L":
                        // 当处理方式为L时，运行PKG_ETL_TAB.P_ETL_TAB_HIST（HIST模式）
                        statement = dbOperate.callProcedure("PKG_ETL_TAB", "P_ETL_TAB_HIST", parameters);
                        V_SUCC_FLAG = statement.getString(6);
                        break;
                }
            }
            // 当作业类型为标准存储过程时
            // 获取存储过程状态，如果存储过程状态不正常，则编译
            // 如果正常，直接运行，否则记录错误
            else if ("1".equals(V_JOB_TYPE)) {
                // 获取存储过程状态
                try {
                    resultSet = dbOperate.select("select a.status  from all_objects a where " +
                            "((a.OBJECT_NAME = '" + I_JOB_NAME + "' and a.OWNER ='" + V_CURRENT_USER + "')" +
                            " or a.OWNER||'.'||a.OBJECT_NAME = '" + I_JOB_NAME + "')" +
                            " and a.OBJECT_TYPE = 'PROCEDURE'");
                    resultSet.next();
                    if ("INVALID".equals(resultSet.getString(1))) {
                        dbOperate.execute("alter procedure '" + I_JOB_NAME + "' compile");
                    }
                    statement = dbOperate.callProcedure("", I_JOB_NAME,
                            new ProcedureParameter(true, V_BEGIN_TIME),
                            new ProcedureParameter(true, V_END_TIME),
                            new ProcedureParameter(true, V_ALL_FLAG),
                            new ProcedureParameter(true, V_BATCH_ID),
                            new ProcedureParameter(false, OracleType.VARCHAR2));
                    V_SUCC_FLAG = statement.getString(5);
                } catch (Exception e) {
                    // TODO 是否要处理失败信息
                    V_SUCC_FLAG = "0";
                }
            }// 当作业类型为数据补差异
            else if ("3".equals(V_JOB_TYPE)) {
                // 获取作业对应的补差异的处理方法
                resultSet = dbOperate.select("select max(CMP_TYPE) from T_CMP_TAB_INFO where JOB_NAME = " + I_JOB_NAME);
                resultSet.next();
                V_CMP_TYPE = resultSet.getString(1);
                switch (V_CMP_TYPE) {
                    case "1":
                        // 当处理方式为1时，运行pkg_cmp_tab.p_cmp_tab_md5（MD5方式）
                        statement = dbOperate.callProcedure("PKG_CMP_TAB", "P_CMP_TAB_MD5", parameters);
                        V_SUCC_FLAG = statement.getString(6);
                        break;
                    case "2":
                        // 当为主键补差异
                        statement = dbOperate.callProcedure("PKG_CMP_TAB", "P_CMP_SNAPSHOOT_REPAIR", parameters);
                        V_SUCC_FLAG = statement.getString(6);
                        break;
                    case "3":
                        // 为SJCK层补数
                        statement = dbOperate.callProcedure("PKG_CMP_TAB", "P_CMP_WAREHOUSE_REPAIR",
                                new ProcedureParameter(true, I_JOB_NAME),
                                new ProcedureParameter(true, V_BATCH_ID),
                                new ProcedureParameter(false, OracleType.VARCHAR2));
                        V_SUCC_FLAG = statement.getString(3);
                        break;
                    case "4":
                        // 当作业类型为关联更新
                        resultSet = dbOperate.select("select case when count(0) >=1 then 'OLD' else 'NEW' end" +
                                " from T_CMP_RELA_MAIN p where p.job_name = " + I_JOB_NAME);
                        resultSet.next();
                        V_RELA_TYPE = resultSet.getString(1);
                        if ("OLD".equals(V_RELA_TYPE)) {
                            statement = dbOperate.callProcedure("PKG_CMP_TAB", "P_CMP_RELA_UPDATE", parameters);
                            V_SUCC_FLAG = statement.getString(6);
                        } else {
                            statement = dbOperate.callProcedure("PKG_CMP_TAB", "P_CMP_RELA_UPDATE2", parameters);
                            V_SUCC_FLAG = statement.getString(6);
                        }
                        break;
                    case "5":
                        // 当作业类型为数据比较
                        statement = dbOperate.callProcedure("PKG_CMP_TAB", "P_QUAL_TAB_COMP", parameters);
                        V_SUCC_FLAG = statement.getString(6);
                        break;
                }
            }
            dbOperate.commit();
            // 记录ETL成功、失败结束的信息
            if ("1".equals(V_SUCC_FLAG)) {
                // 如果外壳调用的作业成功
                V_JOB_STEP = V_SHELL_NAME + "：【调度外壳】：作业正常结束";
                O_SUCC_FLAG = V_SUCC_FLAG;
                log_step(I_JOB_NAME, V_BATCH_ID, V_JOB_STEP, V_RUN_STRING, null, null, null, "1");
                // 外壳调度后续进程，存在相关配置才运行
                resultSet = dbOperate.select(" SELECT COUNT(1)" +
                        "       FROM T_CTL_JOB_FOLLOW T ,\n" +
                        "            T_CTL_FOLLOW T1\n" +
                        "      WHERE T.JOB_NAME = '" + I_JOB_NAME + "'" +
                        " AND T.ENABLE_FLAG = '1'\n" +
                        " AND T.FOLLOW_TYPE = T1.FOLLOW_TYPE\n" +
                        " AND T1.FOLLOW_RUN_SCENE = decode(T1.FOLLOW_RUN_SCENE,2,T1.FOLLOW_RUN_SCENE,'" + V_ALL_FLAG + "')" +
                        " AND T1.FOLLOW_FLAG = '2'" +
                        " AND T1.ENABLE_FLAG = '1'");
                resultSet.next();
                V_NUMBER = resultSet.getInt(1);
                if (V_NUMBER > 0) {
                    V_JOB_STEP = V_SHELL_NAME + "：【调度外壳】：执行后续进程";
                    log_step(I_JOB_NAME, V_BATCH_ID, V_JOB_STEP, "【运行成功】", null, null, null, "1");
                    statement = dbOperate.callProcedure("PKG_CTL_FOLLOW", "P_CTL_FOLLOW_MAIN",
                            new ProcedureParameter(true, I_JOB_NAME),
                            new ProcedureParameter(true, V_BATCH_ID),
                            new ProcedureParameter(true, "2"),
                            new ProcedureParameter(false, OracleType.VARCHAR2));
                    V_SUCC_FLAG = statement.getString(4);
                    O_SUCC_FLAG = V_SUCC_FLAG;
                }
                if ("1".equals(O_SUCC_FLAG)) {
                    // 作业成功就调用周期翻牌的存储过程
                    V_JOB_STEP = V_SHELL_NAME + "：【调度外壳】：作业翻牌成功";
                    dbOperate.callProcedure("PKG_ETL_SHELL", "P_ETL_SHELL_PERIOD",
                            new ProcedureParameter(true, I_JOB_NAME),
                            new ProcedureParameter(true, V_BATCH_ID),
                            new ProcedureParameter(true, V_END_TIME));
                    log_step(I_JOB_NAME, V_BATCH_ID, V_JOB_STEP, "【运行成功】", null, null, null, "1");
                } else {
                    // 后续进程失败则当前job不翻牌
                    V_JOB_STEP = V_SHELL_NAME + "：【调度外壳】：后续进程存在异常";
                    log_step(I_JOB_NAME, V_BATCH_ID, V_JOB_STEP, "【运行成功】", null, null, null, "1");
                }
            } else if ("0".equals(V_SUCC_FLAG)) {
                // 如果外壳调用的作业失败
                V_JOB_STEP = V_SHELL_NAME + "：【调度外壳】：作业异常结束";
                // 返回作业失败标志位
                O_SUCC_FLAG = V_SUCC_FLAG;
                log_step(I_JOB_NAME, V_BATCH_ID, V_JOB_STEP, V_RUN_STRING, null, null, null, "1");
            }
            // 记录作业最终状态及结束日志
            log_stat(I_JOB_NAME, V_SUCC_FLAG, V_BATCH_ID);
            V_JOB_STEP = V_SHELL_NAME + "：【调度外壳正常结束】";
            // 返回外壳成功标志位
            O_SHELL_FLAG = "1";
            log_step(I_JOB_NAME, V_BATCH_ID, V_JOB_STEP, "【调度外壳正常结束】", null, null, null, "1");
        } catch (Exception e) {
            String V_JOB_STEP = V_SHELL_NAME + "：【调度外壳异常结束】";
            // 运行代码
            String v_RUN_CODE = null;
            // 运行错误
            String v_RUN_ERRM = e.getMessage();
            // 返回外壳失败标志位
            O_SHELL_FLAG = "0";
            // 记录错误日志及错误状态
            log_step(I_JOB_NAME, V_BATCH_ID, V_JOB_STEP, V_RUN_STRING, v_RUN_CODE, v_RUN_ERRM, null, O_SHELL_FLAG);
            dbOperate.commit();
        }
    }

    private static void raiseResultException(String I_JOB_NAME, String V_BATCH_ID, String V_SHELL_NAME) throws SQLException, ParameterUseErrorException, UnSupportedFunctionException {
        // 返回作业成功标志位
        O_SUCC_FLAG = "1";
        String V_RUN_STRING = "作业【" + I_JOB_NAME + "】未启用";
        String V_JOB_STEP = V_SHELL_NAME + "：【调度外壳】：作业未启用";
        log_step(I_JOB_NAME, V_BATCH_ID, V_JOB_STEP, V_RUN_STRING, null, null, null, "1");
        // 记录作业最终状态, 3 未作业未启用
        log_stat(I_JOB_NAME, "3", V_BATCH_ID);
        O_SHELL_FLAG = "1";
        // 返回外壳成功标志位
        V_JOB_STEP = V_SHELL_NAME + "：【调度外壳正常结束】";
        log_step(I_JOB_NAME, V_BATCH_ID, V_JOB_STEP, "【调度外壳正常结束】", null, null, null, "1");
        dbOperate.commit();
    }

    private static void raiseResultFollowException(String I_JOB_NAME, String V_BATCH_ID, String V_SHELL_NAME) throws UnSupportedFunctionException, SQLException, ParameterUseErrorException {
        O_SHELL_FLAG = "1";
        log_stat(I_JOB_NAME, "0", V_BATCH_ID);
        String V_JOB_STEP = V_SHELL_NAME + "：【前续进程失败】";
        log_step(I_JOB_NAME, V_BATCH_ID, V_JOB_STEP, "【调度外壳正常结束】", null, null, null, "1");
        dbOperate.commit();
    }

    public String getO_SUCC_FLAG() {
        return O_SUCC_FLAG;
    }

    public String getO_SHELL_FLAG() {
        return O_SHELL_FLAG;
    }
}
