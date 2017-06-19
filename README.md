**Hadoop Mean Temperature**
-------------------------

(DDMMYYY, MIN, MAX) formatinda verilen sicaklik verilerinin ay bazinda max veya min sicaklik degerlerine göre ortalamalarinin bulunmasi. Aşağıdaki ornek min sıcaklık değerlerine gore ortalamayı gösteriyor gibi düşünülebilir.

Bu [linkte](http://archivio-meteo.distile.it/ajax-charts.php?citta_id=4071&dal=01/01/2000&al=31/12/2013&param%5B%5D=tmin&param%5B%5D=tmax) bulunan veriler bir ile ait sıcaklık verilerinin json formatında verilmiş şeklidir. Bu veriler normal bir java programına .txt olarak verilip alınacak olan formatlanmış büyük miktarda veri içeren çıktı hadoop ile işlenmek için  kullanılacaktır. Bu json veriyi normal şekle dönüştürmek için şu kod parçası kullanıldı:

 

01012000,-4.0,5.0
02012000,-5.0,5.1
03012000,-5.0,7.7
04012000,-3.0,9.7
…

    package utils;
    
    import com.fasterxml.jackson.core.JsonFactory;
    import com.fasterxml.jackson.core.JsonParser;
    import com.fasterxml.jackson.core.JsonToken;
    
    import java.io.File;
    
    
    public class JsonToCsvConverter {
    
        public static void main(String[] args) throws Exception {
            System.out.println(JsonToCsvConverter.convert());
        }
    
        public static String convert() throws Exception {
            JsonFactory f = new JsonFactory();
            JsonParser jp = f.createJsonParser(new File("src/main/java/utils/milano_temp.json"));
            StringBuilder builder = new StringBuilder();
            while (jp.nextToken() != JsonToken.END_ARRAY) {
                while (jp.nextToken() != JsonToken.END_OBJECT) {
                    jp.nextToken();
                    while (jp.nextToken() != JsonToken.END_ARRAY) {
    
                        builder.append(getRow(jp));
                    }
                }
            }
            jp.close();
            return builder.toString();
        }
    
        private static String getRow(JsonParser jp) throws Exception {
    
            StringBuilder builder = new StringBuilder();
    
            jp.nextToken();
            jp.nextValue();
            builder.append(jp.getValueAsString()).append(",");
            jp.nextToken();
            jp.nextToken();
            jp.nextValue();
            builder.append(jp.getValueAsDouble()).append(",");
            jp.nextToken();
            jp.nextToken();
            jp.nextValue();
            builder.append(jp.getValueAsDouble()).append("\n");
            jp.nextToken();
    
            return builder.toString();
        }
    }

01012000,0,10.0
02012000,0,20.0
03012000,0,2.0
04012000,0,4.0
05012000,0,3.0

Örnek olarak yukarıdaki verileri dikkate alırsak  iki tane mapper buları şu şekilde paylaşır; ilk maper ilk iki satırı alır ve bunları(10.0 + 20.0) / 2=15.0 olacak şekilde hesaplar ikinci mapper ise kalan 3 veriyi (2.0 + 4.0 + 3.0) / 3=3.0 olarak hesapladıktan sonra reducer sınıfı hesaplanan değerleri alır ve bunları örnek sayısına göre hesaplayarak biraraya getirir. Amaç mapperlarda hesaplama işlemlerinin paralel bir şekilde ilerlemesidir.

Kullanılan mapper sınıfına kısaca bakalım; ilk olarak mapper sınıfına veriler formata uygun olarak ayrılıp ekleniyor. Eğer alınan veriler aşırı miktarda ise bunları map ediyoruz. Ortlamaları hesaplamak için  SumCount sınıfını yazdık bu sınıfa aşağıda linkte verilen kaynak koddan ulaşabilirsiniz.

   

     public static class MeanMapper extends Mapper<Object, Text, Text, SumCount> {
    
        private final int DATE = 0;
        private final int MIN = 1;
        private final int MAX = 2;
    
        private Map<Text, List<Double>> maxMap = new HashMap<>();
     
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    
            String[] values = value.toString().split((","));
    
            if (values.length != 3) {
                return;
            }
    
            String date = values[DATE];
            Text month = new Text(date.substring(2));
            Double max = Double.parseDouble(values[MAX]);
    
            if (!maxMap.containsKey(month)) {
                maxMap.put(month, new ArrayList<Double>());
            }
    
            maxMap.get(month).add(max);
        }
    
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
    
            for (Text month: maxMap.keySet()) {
    
                List<Double> temperatures = maxMap.get(month);
    
                Double sum = 0d;
                for (Double max: temperatures) {
                    sum += max;
                }
    
                context.write(month, new SumCount(sum, temperatures.size()));
            }
        }
    }

Reducer sınıfı basit olarak  mapperlardan dönen işlenmiş SumCount objelerini,ayları ve ortalamaları bir araya getirerek bunları birleştirir.

   

     public static class MeanReducer extends Reducer {
    
        private Map sumCountMap = new HashMap<>();
    
        @Override
        public void reduce(Text key, Iterable values, Context context) throws IOException, InterruptedException {
    
            SumCount totalSumCount = new SumCount();
    
            for (SumCount sumCount : values) {
    
                totalSumCount.addSumCount(sumCount);
            }
    
            sumCountMap.put(new Text(key), totalSumCount);
        }
    
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
    
            for (Text month: sumCountMap.keySet()) {
    
                double sum = sumCountMap.get(month).getSum().get();
                int count = sumCountMap.get(month).getCount().get();
    
                context.write(month, new DoubleWritable(sum/count));
            }
        }
    }

