class BaseService {
  constructor() {
    this.handleError = this.handleError.bind(this);
  }

  /**
   * @param {Error} error 
   * @param {string} context
   * @returns {Error}
   */
  handleError(error, context) {
    console.error(`${context}:`, error);
    return new Error(`Error in ${context}: ${error.message}`);
  }
}

module.exports = BaseService;