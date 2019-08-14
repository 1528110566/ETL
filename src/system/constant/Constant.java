package system.constant;

public enum Constant {
    CONNECTION_ERROR(1, "数据库连接异常"),
    CONNECTION_SUCCESS(2, "数据库连接成功"),
    SQL_SEND_ERROR(3, "SQL发送异常"),
    SQL_SEND_SUCCESS(4, "SQL发送成功"),
    SQL_RUN_ERROR(5, "SQL执行异常"),
    SQL_RUN_SUCCESS(6, "SQL执行成功");

    private Integer index;
    private String message;

    Constant(Integer index, String message) {
        this.index = index;
        this.message = message;
    }

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
