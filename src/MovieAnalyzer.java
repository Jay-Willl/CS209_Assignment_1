import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.stream.*;

public class MovieAnalyzer {
    public Stream<Movie> stream;
    public static List<Movie> temp = new LinkedList<>();

    public class Movie{
        public String posterLink;
        public String seriesTitle;
        public int releasedYear;
        public String certificate;
        public String runtime;
        public List<String> genre;
        public float IMDBRating;
        public String overview;
        public int metaScore;
        public String director;
        public String star1;
        public String star2;
        public String star3;
        public String star4;
        public int noOfVote;
        public int gross;

        public Movie(){

        }

        public Movie(String posterLink, String seriesTitle, int releasedYear, String certificate, String runtime, List<String> genre, float IMDBRating, String overview, int metaScore, String director, String star1, String star2, String star3, String star4, int noOfVote, int gross) {
            this.posterLink = posterLink;
            this.seriesTitle = seriesTitle;
            this.releasedYear = releasedYear;
            this.certificate = certificate;
            this.runtime = runtime;
            this.genre = genre;
            this.IMDBRating = IMDBRating;
            this.overview = overview;
            this.metaScore = metaScore;
            this.director = director;
            this.star1 = star1;
            this.star2 = star2;
            this.star3 = star3;
            this.star4 = star4;
            this.noOfVote = noOfVote;
            this.gross = gross;
        }

        @Override
        public String toString() {
            return "Movie{" +
                    "posterLink='" + posterLink + '\'' +
                    ", seriesTitle='" + seriesTitle + '\'' +
                    ", releasedYear=" + releasedYear +
                    ", certificate='" + certificate + '\'' +
                    ", runtime='" + runtime + '\'' +
                    ", genre=" + genre +
                    ", IMDBRating=" + IMDBRating +
                    ", overview='" + overview + '\'' +
                    ", metaScore=" + metaScore +
                    ", director='" + director + '\'' +
                    ", star1='" + star1 + '\'' +
                    ", star2='" + star2 + '\'' +
                    ", star3='" + star3 + '\'' +
                    ", star4='" + star4 + '\'' +
                    ", noOfVote=" + noOfVote +
                    ", gross=" + gross +
                    '}';
        }
    }

    public MovieAnalyzer(String dataset_path){
        try {
            BufferedReader br = new BufferedReader(new FileReader(dataset_path, StandardCharsets.UTF_8));
            br.readLine(); // skip title
            String line;
            ArrayList<Movie> total = new ArrayList<>();
            while((line = br.readLine()) != null) {
                // import a row into the list
                ArrayList<String> list = new ArrayList<>();
                String[] parts = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                // handle list.get(15)/gross
                int gross = 0;
                if(parts.length == 15 || parts[15].equals("")){
                    gross = -1;
                }else{
                    gross = Integer.parseInt(parts[15].replaceAll("^\"*|\"*$", "").replace(",", ""));
                }
                // parts[0] handle ""
                String posterLink = parts[0].replaceAll("^\"*|\"*$", "");
                // parts[1] handle ""
                String serialTitle = parts[1].replaceAll("^\"*|\"*$", "");
                // parts[2] handle null situation
                int releaseYear = 0;
                if(!Objects.equals(parts[2], "")){
                    releaseYear = Integer.parseInt(parts[2]);
                }else{
                    releaseYear = -1;
                }
                // parts[5] -> List
                List<String> genre = new ArrayList<>();
                genre = List.of(parts[5].replace("\"", "").split(", "));
                // list.get(7) handle ""
                String overview = parts[7].replaceAll("^\"*|\"*$", "");
//                overview = overview.replace("\"\"", "\"");
                // list.get(8) handle ""
                int metaScore = 0;
                if(Objects.equals(parts[8], "")){
                    metaScore = -1;
                }else{
                    metaScore = Integer.parseInt(parts[8]);
                }
                Movie movie = new Movie(posterLink, serialTitle, releaseYear, parts[3],
                        parts[4], genre, Float.parseFloat(parts[6]), overview,
                        metaScore, parts[9], parts[10], parts[11],
                        parts[12], parts[13], Integer.parseInt(parts[14]), gross);
                temp.add(movie);



//                if(line.endsWith(",")){
//                    line = line.concat(",123");
//                }
//                if(line.contains("\"\"")){
//                    line = line.replace("\"\"", ",*");
//                }
//                String[] parts = line.split(",");
//                Deque<String> dq = new LinkedList<>();
//                for (String part : parts) {
//                    if (part.contains("\"")) {
//                        if (dq.isEmpty()) {
//                            dq.push(part);
//                        } else {
//                            String assemble = "";
//                            while (!dq.isEmpty()) {
//                                assemble = assemble.concat(dq.pollLast());
//                            }
//                            assemble = assemble.concat(",");
//                            assemble = assemble.concat(part);
//                            list.add(assemble);
//                        }
//                    } else {se
//                        if (!dq.isEmpty()) {
//                            dq.push(",");
//                            dq.push(part);
//                        } else {
//                            list.add(part);
//                        }
//                    }
//                }
//
//                // list.get(5) -> List
//                List<String> genre = new ArrayList<>();
//                genre = List.of(list.get(5).replace("\"", "").split(", "));
//                // list.get(7) handle ""
//                String overview = list.get(7).replaceAll("^\"*|\"*$", "");
//                // list.get(8) handle ""
//                int metaScore = 0;
//                if(Objects.equals(list.get(8), "")){
//                    metaScore = -1;
//                }else{
//                    metaScore = Integer.parseInt(list.get(8));
//                }
//                // list.get(15) -> int
//                int gross = 0;
//                if(!Objects.equals(list.get(15), "")){
//                    gross = Integer.parseInt(list.get(15).replace("\"", "").replace(",", ""));
//                }else{
//                    gross = -1;
//                }
//                Movie movie = new Movie(posterLink, serialTitle, releaseYear, list.get(3),
//                        list.get(4), genre, Float.parseFloat(list.get(6)), overview,
//                        metaScore, list.get(9), list.get(10), list.get(11),
//                        list.get(12), list.get(13), Integer.parseInt(list.get(14)), gross);
//                temp.add(movie);
            }
            stream = temp.stream();
            //temp = stream.toList();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public Map<Integer, Integer> getMovieCountByYear(){
        Stream<Movie> temp_stream = temp.stream();
        Map<Integer, Integer> res = temp_stream
                .collect(Collectors.groupingBy(item -> (item.releasedYear), Collectors.reducing(0, e -> 1, Integer::sum)));
        return res.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.reverseOrder()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (v1, v2) -> { throw new IllegalStateException(); },
                        LinkedHashMap::new
                ));
    }

    public Map<String, Integer> getMovieCountByGenre(){
        Stream<Movie> temp_stream = temp.stream();
        Map<String, Integer> res = temp_stream
                .flatMap(item -> item.genre.stream())
                .collect(Collectors.groupingBy(
                        Function.identity(),
                        Collectors.reducing(0, e -> 1, Integer::sum)
                ));
        return res.entrySet().stream()
                .sorted(
                        (item1, item2) -> {
                            if(Objects.equals(item1.getValue(), item2.getValue())){
                                return item1.getKey().compareTo(item2.getKey());
                            }else{
                                return item2.getValue() - item1.getValue();
                            }
                        }
                ).collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (v1, v2) -> { throw new IllegalStateException(); },
                        LinkedHashMap::new
                ));
    }

    public Map<List<String>, Integer> getCoStarCount(){
        Stream<Movie> temp_stream = temp.stream();
        Map<List<String>, Integer> res = temp_stream
                .flatMap(item -> flatCoStar(item).stream())
                .collect(Collectors.groupingBy(
                        Function.identity(),
                        Collectors.reducing(0, e -> 1, Integer::sum)
                ));
//        stream.flatMap(item -> flatCoStar(item).stream())
//                .forEach(System.out::println);
        return res.entrySet().stream()
                .sorted(
                        (item1, item2) -> {
                            if(Objects.equals(item1.getValue(), item2.getValue())){
                                if(Objects.equals(item1.getKey().get(0), item2.getKey().get(0))){
                                    return item1.getKey().get(1).compareTo(item2.getKey().get(1));
                                }else{
                                    return item1.getKey().get(0).compareTo(item2.getKey().get(0));
                                }
                            }else{
                                return item2.getValue() - item1.getValue();
                            }
                        }
                )
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (v1, v2) -> { throw new IllegalStateException(); },
                        LinkedHashMap::new
                ));
    }

    public List<List<String>> flatCoStar(Movie movie){
        List<List<String>> res = new LinkedList<>();
        LinkedList<String> temp1 = new LinkedList<>();
        sortedAdd(temp1, movie.star1, movie.star2);
        res.add(temp1);
        LinkedList<String> temp2 = new LinkedList<>();
        sortedAdd(temp2, movie.star1, movie.star3);
        res.add(temp2);
        LinkedList<String> temp3 = new LinkedList<>();
        sortedAdd(temp3, movie.star1, movie.star4);
        res.add(temp3);
        LinkedList<String> temp4 = new LinkedList<>();
        sortedAdd(temp4, movie.star2, movie.star3);
        res.add(temp4);
        LinkedList<String> temp5 = new LinkedList<>();
        sortedAdd(temp5, movie.star2, movie.star4);
        res.add(temp5);
        LinkedList<String> temp6 = new LinkedList<>();
        sortedAdd(temp6, movie.star3, movie.star4);
        res.add(temp6);
        return res;
    }

    public void sortedAdd(LinkedList<String> temp, String item1, String item2){
        if(item1.compareTo(item2) < 0){
            temp.add(item1);
            temp.add(item2);
        }else{
            temp.add(item2);
            temp.add(item1);
        }
    }

    public List<String> getTopMovies(int top_k, String by){
        Stream<Movie> temp_stream = temp.stream();
        List<String> res = null;
        if(Objects.equals(by, "runtime")){
            res = temp_stream
                    .sorted((item1, item2) -> {
                        int runtime1 = Integer.parseInt(item1.runtime.replace(" min", ""));
                        int runtime2 = Integer.parseInt(item2.runtime.replace(" min", ""));
                        if(runtime1 == runtime2){
                            return item1.seriesTitle.compareTo(item2.seriesTitle);
                        }
                        return runtime2 - runtime1;
                    })
                    .map(item -> item.seriesTitle)
                    .limit(top_k)
                    .toList();
        }else if(Objects.equals(by, "overview")){
            res = temp_stream
                    .sorted((item1, item2) -> {
                        int len1 = item1.overview.length();
                        int len2 = item2.overview.length();
                        if(len1 == len2){
                            return item1.seriesTitle.compareTo(item2.seriesTitle);
                        }
                        return len2 - len1;
                    })
                    .map(item -> item.seriesTitle)
                    .limit(top_k)
                    .toList();
        }
        return res;
    }

    public List<String> getTopStars(int top_k, String by){
        Stream<Movie> temp_stream = temp.stream();
        List<String> res = null;
        if(Objects.equals(by, "rating")){
            Map<String, Double> overall = temp_stream
                    .map(item -> {
                        List<info> temp = new LinkedList<>();
                        temp.add(new info(item.star1, item.IMDBRating));
                        temp.add(new info(item.star2, item.IMDBRating));
                        temp.add(new info(item.star3, item.IMDBRating));
                        temp.add(new info(item.star4, item.IMDBRating));
                        return temp;
                    })
                    .flatMap(Collection::stream)
                    .collect(Collectors.groupingBy(info::getName,
                            Collectors.averagingDouble(info::getRating)));
            res = overall
                    .entrySet().stream()
                    .sorted((item1, item2) -> {
                        if(Objects.equals(item1.getValue(), item2.getValue())){
                            return item1.getKey().compareTo(item2.getKey());
                        }else{
                            if(item2.getValue() - item1.getValue() >= 0){
                                return 1;
                            }else{
                                return -1;
                            }
                        }
                    })
                    .map(item -> item.getKey())
                    .limit(top_k)
                    .collect(Collectors.toList());
        }else if(Objects.equals(by, "gross")){
            Map<String, Double> overall = temp_stream
                    .filter(item -> item.gross != -1)
                    .map(item -> {
                        List<info2> temp = new LinkedList<>();
                        temp.add(new info2(item.star1, item.gross));
                        temp.add(new info2(item.star2, item.gross));
                        temp.add(new info2(item.star3, item.gross));
                        temp.add(new info2(item.star4, item.gross));
                        return temp;
                    })
                    .flatMap(Collection::stream)
                    .collect(Collectors.groupingBy(info2::getName,
                            Collectors.averagingDouble(info2::getGross)));
            res = overall
                    .entrySet().stream()
                    .sorted((item1, item2) -> {
                        if(Objects.equals(item1.getValue(), item2.getValue())){
                            return item1.getKey().compareTo(item2.getKey());
                        }else{
                            if(item2.getValue() - item1.getValue() > 0){
                                return 1;
                            }else{
                                return -1;
                            }
                        }
                    })
                    .map(item -> item.getKey())
                    .limit(top_k)
                    .collect(Collectors.toList());
        }
        return res;
    }

    public class info{
        public String name;
        public Float rating;

        public String getName() {
            return name;
        }

        public Float getRating() {
            return rating;
        }

        public info(String name, Float rating){
            this.name = name;
            this.rating = rating;
        }
    }

    public class info2{
        public String name;
        public Integer gross;

        public String getName() {
            return name;
        }

        public Integer getGross() {
            return gross;
        }

        public info2(String name, Integer gross) {
            this.name = name;
            this.gross = gross;
        }
    }


    public List<String> getStarList(Movie movie){
        List<String> res = new LinkedList<>();
        res.add(movie.star1);
        res.add(movie.star2);
        res.add(movie.star3);
        res.add(movie.star4);
        return res;
    }

    public List<String> searchMovies(String genre, float min_rating, int max_runtime){
        Stream<Movie> temp_stream = temp.stream();
        List<String> res = temp_stream
                .filter(item -> item.genre.contains(genre))
                .filter(item -> item.IMDBRating >= min_rating)
                .filter(item -> Integer.parseInt(item.runtime.replace(" min", "")) <= max_runtime)
                .sorted((item1, item2) -> {
                    return item1.seriesTitle.compareTo(item2.seriesTitle);
                })
                .map(item -> item.seriesTitle)
                .sorted(
                        Comparator.naturalOrder()
                )
                .toList();
        return res;
    }


//    public static void main(String[] args) throws IOException {
//        MovieAnalyzer movieAnalyzer = new MovieAnalyzer("/Users/blank/repo/Java 2 Assign 1/resources/imdb_top_500.csv");
//        temp.forEach(item -> System.out.println(item));
////        System.out.println(movieAnalyzer.getMovieCountByYear());
////        System.out.println(movieAnalyzer.getMovieCountByGenre());
////        System.out.println(movieAnalyzer.getCoStarCount().toString());
////        System.out.println(movieAnalyzer.getTopMovies(20, "overview"));
////        System.out.println(movieAnalyzer.getTopStars(20, "rating"));
////        System.out.print(movieAnalyzer.getTopStars(100, "gross"));
////        System.out.println(movieAnalyzer.searchMovies("Adventure", 8.0F, 150));
//    }
//
//    private static List<String> splitInput (String input) {
//        var chars = input.chars().toArray();
//        //var it = chars.iterator();
//        var it = 0;
//        var ans = new ArrayList<String> () ;
//        var tmp = new String();
//        var is_quoted = false;
//        while (it < chars.length) {
//            var now = chars[it];
//            it++;
//            switch (now) {
//                case (int)'"':
//                    {
//                        if (is_quoted) {
//                            if (it < chars.length && chars[it] == '"') {
//                                it++;
//                                tmp += '"';
//                                is_quoted = false;
//                            }
//                        }
//                        is_quoted = !is_quoted;
//                    }
//                case (int )',':
//                    if (!is_quoted) {
//                        ans.add(tmp);
//                        tmp = new String();
//                    } else {
//                        tmp += ',';
//                    }
//                default:
//                    tmp += (char ) now;
//            }
//        }
//        ans.add(tmp);
//        assert !is_quoted;
//        return ans;
//    }

}