package Models;

public class Results {
	

	private String date;
	private String home_team;
	private String away_team;
	private String home_score;
	private String away_score;
	private String tournament;
	private String city;
	private String country;
	private boolean neutral;
	

	public Results(String date, String home_team, String away_team, String home_score, String away_score, String tournament,
			String city, String country, boolean neutral) {
		super();
		this.date = date;
		this.home_team = home_team;
		this.away_team = away_team;
		this.home_score = home_score;
		this.away_score = away_score;
		this.tournament = tournament;
		this.city = city;
		this.country = country;
		this.neutral = neutral;
	}
	
	public String getDate() {
		return date;
	}


	public void setDate(String date) {
		this.date = date;
	}


	public String getHome_team() {
		return home_team;
	}


	public void setHome_team(String home_team) {
		this.home_team = home_team;
	}


	public String getAway_team() {
		return away_team;
	}


	public void setAway_team(String away_team) {
		this.away_team = away_team;
	}


	public String getHome_score() {
		return home_score;
	}


	public void setHome_score(String home_score) {
		this.home_score = home_score;
	}


	public String getAway_score() {
		return away_score;
	}


	public void setAway_score(String away_score) {
		this.away_score = away_score;
	}


	public String getTournament() {
		return tournament;
	}


	public void setTournament(String tournament) {
		this.tournament = tournament;
	}


	public String getCity() {
		return city;
	}


	public void setCity(String city) {
		this.city = city;
	}


	public String getCountry() {
		return country;
	}


	public void setCountry(String country) {
		this.country = country;
	}


	public boolean getNeutral() {
		return neutral;
	}


	public void setNeutral(boolean neutral) {
		this.neutral = neutral;
	}
	




	@Override
	public String toString() {
		return "results [date=" + date + ", home_team=" + home_team + ", away_team=" + away_team + ", home_score="
				+ home_score + ", away_score=" + away_score + ", tournament=" + tournament + ", city=" + city
				+ ", country=" + country + ", neutral=" + neutral + "]";
	}

	


}