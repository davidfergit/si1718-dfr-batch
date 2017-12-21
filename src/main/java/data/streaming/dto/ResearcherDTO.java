package data.streaming.dto;

public class ResearcherDTO {

	private String firstResearcher;
	private String secondResearcher;
	private Integer rating;
	
	public ResearcherDTO() {
		
	}
	
	public ResearcherDTO(String firstResearcher, String secondResearcher, Integer rating) {
		super();
		this.firstResearcher = firstResearcher;
		this.secondResearcher = secondResearcher;
		this.rating = rating;
	}

	public String getFirstResearcher() {
		return firstResearcher;
	}

	public void setFirstResearcher(String firstResearcher) {
		this.firstResearcher = firstResearcher;
	}

	public String getSecondResearcher() {
		return secondResearcher;
	}

	public void setSecondResearcher(String secondResearcher) {
		this.secondResearcher = secondResearcher;
	}

	public Integer getRating() {
		return rating;
	}

	public void setRating(Integer rating) {
		this.rating = rating;
	}

	@Override
	public String toString() {
		return "ResearcherDTO [firstResearcher=" + firstResearcher + ", secondResearcher=" + secondResearcher
				+ ", rating=" + rating + "]";
	}
	
}
