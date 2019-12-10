package solution;

import java.math.BigDecimal;

public class DecimalUtils {
	
		static double val=1.0;
	public static double round(double value, int numberOfDigitsAfterDecimalPoint) {
		 
		if ((Double) value instanceof Double) {
		             if ((Double) value instanceof Double && !((Double)value).isInfinite() && !((Double)value).isNaN()) {
		BigDecimal bigDecimal = new BigDecimal(value);
        bigDecimal = bigDecimal.setScale(numberOfDigitsAfterDecimalPoint,
                BigDecimal.ROUND_HALF_UP);
         return val = bigDecimal.doubleValue();
    }
		             


		
	}
			return val;
	}
		
}

