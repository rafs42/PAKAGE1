package COM.ACME;

public class testREGEX {
    public static void main(String[] args) {
        System.out.println("Hello, World!");
        String vHTYPE;
        String vHUNIT;
        String vHDATE;
        int vHREC_COUNT;
        String vEVENT;
        String vCIF;
        //String HeadRegex = "APPLEINCENTIVEPAYOUT\\|BH\\|[0-9]{8}\\|[0-9]{10}";
        String HeadRegex = "(APPLEINCENTIVEPAYOUT)(\\|BH)(\\|[0-9]{8})(\\|[0-9]{10})";
        String aReq = "APPLEINCENTIVEPAYOUT|BH|20201016|0000000005";
        if (aReq.matches(HeadRegex)) {
            System.out.println("matchches");
            String[] Header;
            Header = aReq.split(HeadRegex);
            System.out.println(Header.length);
            for (String item : Header) {
                System.out.println(item);
            }
        } else {
            System.out.println("nomatch");
        }


        String[] sHDR = aReq.split("\\|");
        int hcount = 0;
        for (String hField : sHDR) {
            System.out.println(hField);
            switch (hcount) {
                case 0:
                    vHTYPE = hField;
                    break;
                case 1:
                    vHUNIT = hField;
                    break;
                case 2:
                    vHDATE = hField;
                    break;
                case 3:
                    vHREC_COUNT = Integer.parseInt(hField);
                    break;
            }
            hcount++;
        }
    }

}
